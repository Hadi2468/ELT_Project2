import os
import boto3
import io
import json
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# =====================
# Setup logging
# =====================
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# =====================
# Environment Variables
# =====================
BRONZE_BUCKET = os.environ["BRONZE_BUCKET"]
SERVICE_ACCOUNT_JSON = os.environ["SERVICE_ACCOUNT_JSON"]
GLUE_JOB_NAME = "project2-silver-glue"
GLUE_INPUT_PATH = f"s3://{BRONZE_BUCKET}/raw-data/"

# =====================
# Google Drive Setup
# =====================
credentials_info = json.loads(SERVICE_ACCOUNT_JSON)
credentials = service_account.Credentials.from_service_account_info(
    credentials_info,
    scopes=["https://www.googleapis.com/auth/drive.readonly"]
)

drive_service = build("drive", "v3", credentials=credentials)

# =====================
# AWS Clients
# =====================
s3_client = boto3.client('s3')
glue_client = boto3.client("glue")

# =====================
# Files to Download
# =====================
FILES = [
    {
        "file_id": "1kZMZFGfTLdcwmdhjDPZh2-XE2_gOBRCz",
        "s3_key": "raw-data/PBJ_Daily_Nurse_Staffing_Q2_2024.csv"
    },
    {
        "file_id": "1gsofjXa-DHRPgPw0iZQa3cl4VqVP74jb",
        "s3_key": "raw-data/NH_ProviderInfo_Oct2024.csv"
    }
]

# =====================
# Helper Functions
# =====================
def download_drive_file_to_s3(file_id, s3_key):
    """Download a Google Drive file and upload it to S3."""
    try:
        logger.info(f"Starting download: {file_id} → {s3_key}")
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False

        while not done:
            status, done = downloader.next_chunk()
            if status:
                logger.info(f"Download progress: {int(status.progress() * 100)}%")

        fh.seek(0)
        s3_client.upload_fileobj(fh, BRONZE_BUCKET, s3_key)
        logger.info(f"✅ Uploaded {s3_key} to s3://{BRONZE_BUCKET}/")

    except Exception as e:
        logger.error(f"❌ Error downloading/uploading file {file_id} → {s3_key}: {str(e)}")
        raise

# =====================
# Lambda Handler
# =====================
def lambda_handler(event, context):
    logger.info("Starting Google Drive → S3 ingestion")

    try:
        # Upload all files
        for file in FILES:
            download_drive_file_to_s3(file["file_id"], file["s3_key"])

        logger.info("✅ All files uploaded successfully")

        # Trigger Glue Workflow
        try:
            logger.info("Triggering Glue workflow")
            response = glue_client.start_workflow_run(
                Name="project2-bronze-silver-gold",
                RunProperties={
                    "input_path": f"s3://{BRONZE_BUCKET}/raw-data/"
                }
            )
            logger.info(f"✅ Glue workflow triggered: {response['RunId']}")

        except Exception as glue_error:
            logger.error(f"❌ Failed to trigger Glue workflow: {str(glue_error)}")
            raise

        return {
            "statusCode": 200,
            "body": "Files uploaded and Glue workflow triggered successfully"
        }

    except Exception as e:
        logger.error(f"❌ Lambda execution failed: {str(e)}")
        return {
            "statusCode": 500,
            "body": str(e)
        }
