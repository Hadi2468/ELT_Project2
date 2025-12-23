import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col

# ----------------------------------------------------
# Glue setup
# ----------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

INPUT_PATH = args['input_path']  # Passed from Lambda
BRONZE_BUCKET = INPUT_PATH  # Can use to read all files dynamically

# ----------------------------------------------------
# Helper function to normalize column names
# ----------------------------------------------------
def normalize_columns(df):
    for c in df.columns:
        df = df.withColumnRenamed(
            c,
            c.upper().replace(" ", "_")
        )
    return df

# ====================================================
# FACT TABLE
# ====================================================
# Read all files matching pattern
fact_input_path = f"{BRONZE_BUCKET}PBJ_Daily_Nurse_Staffing_Q2_2024.csv"
fact_output_path = "s3://project2-healthcare-silver-bucket/fact/fact_table/"

fact_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(fact_input_path)
)

# Drop unwanted columns
fact_df = fact_df.drop("COUNTY_NAME", "COUNTY_FIPS", "CY_Qtr")

# Normalize column names
fact_df = normalize_columns(fact_df)

# Write as Parquet
(
    fact_df
    .write
    .mode("overwrite")
    .parquet(fact_output_path)
)

# ====================================================
# DIM TABLE
# ====================================================
dim_input_path = f"{BRONZE_BUCKET}NH_ProviderInfo_Oct2024.csv"
dim_output_path = "s3://project2-healthcare-silver-bucket/dim/dim_table/"

dim_columns = [
    "CMS Certification Number (CCN)",
    "Provider Name",
    "Provider Type",
    "State",
    "City/Town",
    "ZIP Code",
    "Number of Certified Beds",
    "Average Number of Residents per Day",
    "Average Number of Residents per Day Footnote",
    "Reported Nurse Aide Staffing Hours per Resident per Day",
    "Reported LPN Staffing Hours per Resident per Day",
    "Reported RN Staffing Hours per Resident per Day",
    "Reported Licensed Staffing Hours per Resident per Day",
    "Reported Total Nurse Staffing Hours per Resident per Day",
    "Total number of nurse staff hours per resident per day on the weekend",
    "Registered Nurse hours per resident per day on the weekend",
    "Reported Physical Therapist Staffing Hours per Resident Per Day",
    "Total nursing staff turnover",
    "Total nursing staff turnover footnote",
    "Registered Nurse turnover",
    "Registered Nurse turnover footnote",
    "Number of administrators who have left the nursing home",
    "Administrator turnover footnote"
]

dim_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(dim_input_path)
)

# Select required columns
dim_df = dim_df.select(*dim_columns)

# Rename specific columns
dim_df = (
    dim_df
    .withColumnRenamed("CMS Certification Number (CCN)", "Provider_num")
    .withColumnRenamed("City/Town", "CITY_TOWN")
)

# Normalize column names
dim_df = normalize_columns(dim_df)

# Write as Parquet
(
    dim_df
    .write
    .mode("overwrite")
    .parquet(dim_output_path)
)

# ====================================================
# Finish Glue Job
# ====================================================
job.commit()
