import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import (col, expr, coalesce, lit, when, round)

# ---------------------------------------------------
# Glue boilerplate
# ---------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # ---------------------------------------------------
    # Read source dim_table
    # ---------------------------------------------------
    print("Reading dim_table from silver bucket")
    dim_df = spark.read.parquet(
        "s3://project2-healthcare-silver-bucket/dim/dim_table/"
    )

    # ===================================================
    # BUILD dim_bed
    # ===================================================
    print("Building dim_bed")
    bed_df = dim_df.select(
        col('PROVIDER_NUM'),
        col('PROVIDER_NAME'),
        col('PROVIDER_TYPE'),
        col('STATE'),
        col('NUMBER_OF_CERTIFIED_BEDS'),
        coalesce(col('AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY'), lit(0))
            .alias('AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY'),
        coalesce(col('AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY_FOOTNOTE'), lit(0))
            .alias('AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY_FOOTNOTE')
    ).withColumn(
        "AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY_TOTAL",
        col('AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY')
        + col('AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY_FOOTNOTE')
    ).withColumn(
        "BED_UTILIZATION_RATE",
        expr("""
            CASE 
                WHEN NUMBER_OF_CERTIFIED_BEDS = 0 THEN 0
                ELSE 100 * AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY_TOTAL
                     / NUMBER_OF_CERTIFIED_BEDS
            END
        """)
    ).withColumn(
        "BED_UTILIZATION_RATE",
        round(col("BED_UTILIZATION_RATE"), 2)
    )

    print("Writing dim_bed to gold bucket")
    bed_df.write.mode("overwrite").parquet(
        "s3://project2-healthcare-gold-bucket/dim_bed/"
    )

    # ===================================================
    # BUILD dim_nurse
    # ===================================================
    print("Building dim_nurse")
    nurse_df = dim_df.select(
        col('PROVIDER_NUM'),
        col('PROVIDER_NAME'),
        col('PROVIDER_TYPE'),
        col('STATE'),
        coalesce(col('REPORTED_NURSE_AIDE_STAFFING_HOURS_PER_RESIDENT_PER_DAY'), lit(0))
            .alias('REPORTED_NURSE_AIDE_STAFFING_HOURS_PER_RESIDENT_PER_DAY'),
        coalesce(col('REPORTED_LPN_STAFFING_HOURS_PER_RESIDENT_PER_DAY'), lit(0))
            .alias('REPORTED_LPN_STAFFING_HOURS_PER_RESIDENT_PER_DAY'),
        coalesce(col('REPORTED_RN_STAFFING_HOURS_PER_RESIDENT_PER_DAY'), lit(0))
            .alias('REPORTED_RN_STAFFING_HOURS_PER_RESIDENT_PER_DAY'),
        coalesce(col('REPORTED_TOTAL_NURSE_STAFFING_HOURS_PER_RESIDENT_PER_DAY'), lit(0))
            .alias('REPORTED_TOTAL_NURSE_STAFFING_HOURS_PER_RESIDENT_PER_DAY'),
        coalesce(col('TOTAL_NUMBER_OF_NURSE_STAFF_HOURS_PER_RESIDENT_PER_DAY_ON_THE_WEEKEND'), lit(0))
            .alias('TOTAL_NUMBER_OF_NURSE_STAFF_HOURS_PER_RESIDENT_PER_DAY_ON_THE_WEEKEND'),
        coalesce(col('REGISTERED_NURSE_HOURS_PER_RESIDENT_PER_DAY_ON_THE_WEEKEND'), lit(0))
            .alias('REGISTERED_NURSE_HOURS_PER_RESIDENT_PER_DAY_ON_THE_WEEKEND')
    ).withColumn(
        'NURSE_AIDE_TO_PATIENT_RATE',
        (5 * col('REPORTED_NURSE_AIDE_STAFFING_HOURS_PER_RESIDENT_PER_DAY')
         + 2 * col('TOTAL_NUMBER_OF_NURSE_STAFF_HOURS_PER_RESIDENT_PER_DAY_ON_THE_WEEKEND')) / 7
    ).withColumn(
        'LPN_TO_PATIENT_RATE',
        (5 * col('REPORTED_LPN_STAFFING_HOURS_PER_RESIDENT_PER_DAY')
         + 2 * col('TOTAL_NUMBER_OF_NURSE_STAFF_HOURS_PER_RESIDENT_PER_DAY_ON_THE_WEEKEND')) / 7
    ).withColumn(
        'RN_TO_PATIENT_RATE',
        (5 * col('REPORTED_RN_STAFFING_HOURS_PER_RESIDENT_PER_DAY')
         + 2 * col('REGISTERED_NURSE_HOURS_PER_RESIDENT_PER_DAY_ON_THE_WEEKEND')) / 7
    ).withColumn(
        'NURSE_TO_PATIENT_RATE',
        (5 * col('REPORTED_TOTAL_NURSE_STAFFING_HOURS_PER_RESIDENT_PER_DAY')
         + 2 * col('TOTAL_NUMBER_OF_NURSE_STAFF_HOURS_PER_RESIDENT_PER_DAY_ON_THE_WEEKEND')) / 7
    ).withColumn('NURSE_AIDE_TO_PATIENT_RATE', round(col('NURSE_AIDE_TO_PATIENT_RATE'), 2)) \
     .withColumn('LPN_TO_PATIENT_RATE', round(col('LPN_TO_PATIENT_RATE'), 2)) \
     .withColumn('RN_TO_PATIENT_RATE', round(col('RN_TO_PATIENT_RATE'), 2)) \
     .withColumn('NURSE_TO_PATIENT_RATE', round(col('NURSE_TO_PATIENT_RATE'), 2))

    print("Writing dim_nurse to gold bucket")
    nurse_df.write.mode("overwrite").parquet(
        "s3://project2-healthcare-gold-bucket/dim_nurse/"
    )

    # ---------------------------------------------------
    # Commit job ONLY if everything succeeded
    # ---------------------------------------------------
    job.commit()
    print("✅ Glue job completed successfully")

except Exception as e:
    print(f"❌ Glue job failed: {str(e)}")
    raise