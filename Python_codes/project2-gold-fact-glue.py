import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import (col, expr, when, round, to_date, year, month, dayofmonth)

# ---------------------------------------------------
# Glue boilerplate
# ---------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ---------------------------------------------------
# Read source fact_table ONCE
# ---------------------------------------------------
fact_df = spark.read.parquet("s3://project2-healthcare-silver-bucket/fact/fact_table/")

# ===================================================
# BUILD fact_staff
# ===================================================
staff_df = fact_df.select(
    col('PROVNUM'),
    col('PROVNAME'),
    col('CITY'),
    col('STATE'),
    col('WORKDATE'),
    col('MDSCENSUS'),
    col('HRS_RNDON_EMP'),
    col('HRS_RNDON_CTR'),
    col('HRS_RNADMIN_EMP'),
    col('HRS_RNADMIN_CTR'),   
    col('HRS_RN_EMP'),
    col('HRS_RN_CTR'),
    col('HRS_LPNADMIN_EMP'),
    col('HRS_LPNADMIN_CTR'),
    col('HRS_LPN_EMP'),
    col('HRS_LPN_CTR'),
    col('HRS_CNA_EMP'),
    col('HRS_CNA_CTR'),
    col('HRS_NATRN_EMP'),
    col('HRS_NATRN_CTR'),
    col('HRS_MEDAIDE_EMP'),
    col('HRS_MEDAIDE_CTR')

).withColumn('WORKDATE', to_date(col('WORKDATE').cast('string'), 'yyyyMMdd')    
).withColumn('WORKYEAR', year('WORKDATE')
).withColumn('WORKMONTH', month('WORKDATE')
).withColumn('WORKDAY', dayofmonth('WORKDATE')

).withColumn(
    'STAFF_RATIO_RNDON',
    expr("""
        CASE 
            WHEN HRS_RNDON_CTR < 1e-3 THEN 10000
            ELSE HRS_RNDON_EMP / HRS_RNDON_CTR
        END
    """)
).withColumn(
    'STAFF_RATIO_RNDON',
    round(col('STAFF_RATIO_RNDON'), 2)
).withColumn(
    'STAFF_RATIO_RNADMIN',
    expr("""
        CASE 
            WHEN HRS_RNADMIN_CTR < 1e-3 THEN 10000
            ELSE HRS_RNADMIN_EMP / HRS_RNADMIN_CTR
        END
    """)
).withColumn(
    'STAFF_RATIO_RNADMIN',
    round(col('STAFF_RATIO_RNADMIN'), 2)
).withColumn(
    'STAFF_RATIO_RN',
    expr("""
        CASE 
            WHEN HRS_RN_CTR < 1e-3 THEN 10000
            ELSE HRS_RN_EMP / HRS_RN_CTR
        END
    """)
).withColumn(
    'STAFF_RATIO_RN',
    round(col('STAFF_RATIO_RN'), 2)
).withColumn(
    'STAFF_RATIO_LPNADMIN',
    expr("""
        CASE 
            WHEN HRS_LPNADMIN_CTR < 1e-3 THEN 10000
            ELSE HRS_LPNADMIN_EMP / HRS_LPNADMIN_CTR
        END
    """)
).withColumn(
    'STAFF_RATIO_LPNADMIN',
    round(col('STAFF_RATIO_LPNADMIN'), 2)
).withColumn(
    'STAFF_RATIO_LPN',
    expr("""
        CASE 
            WHEN HRS_LPN_CTR < 1e-3 THEN 10000
            ELSE HRS_LPN_EMP / HRS_LPN_CTR
        END
    """)
).withColumn(
    'STAFF_RATIO_LPN',
    round(col('STAFF_RATIO_LPN'), 2)
).withColumn(
    'STAFF_RATIO_CNA',
    expr("""
        CASE 
            WHEN HRS_CNA_CTR < 1e-3 THEN 10000
            ELSE HRS_CNA_EMP / HRS_CNA_CTR
        END
    """)
).withColumn(
    'STAFF_RATIO_CNA',
    round(col('STAFF_RATIO_CNA'), 2)
).withColumn(
    'STAFF_RATIO_NATRN',
    expr("""
        CASE 
            WHEN HRS_NATRN_CTR < 1e-3 THEN 10000
            ELSE HRS_NATRN_EMP / HRS_NATRN_CTR
        END
    """)
).withColumn(
    'STAFF_RATIO_NATRN',
    round(col('STAFF_RATIO_NATRN'), 2)
).withColumn(
    'STAFF_RATIO_MEDAIDE',
    expr("""
        CASE 
            WHEN HRS_MEDAIDE_CTR < 1e-3 THEN 10000
            ELSE HRS_MEDAIDE_EMP / HRS_MEDAIDE_CTR
        END
    """)
).withColumn(
    'STAFF_RATIO_MEDAIDE',
    round(col('STAFF_RATIO_MEDAIDE'), 2)
)

# Write fact_staff
staff_df.write.mode("overwrite").parquet("s3://project2-healthcare-gold-bucket/fact_staff/")

# ---------------------------------------------------
# Finish job
# ---------------------------------------------------
job.commit()
