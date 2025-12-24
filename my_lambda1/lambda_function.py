import os
import json
import traceback
from dotenv import load_dotenv
from bson import ObjectId

from textract_service import run_chunkr
from mongo import (
    mark_file_as_failed,
    update_job_status,
    get_mongo_collection,
    delete_credit_record
)

load_dotenv()
S3_BUCKET_NAME = "yellow-checks-test"


def get_original_filename_from_mongo(file_id: str) -> str:
    collection = get_mongo_collection("tb_file_details")
    doc = collection.find_one({"_id": ObjectId(file_id)})

    if not doc:
        raise ValueError(f"No file found for file_id: {file_id}")

    return doc["originalS3File"]


def lambda_handler(event, context):
    page_count = 0
    pages = None
    credit_deleted = False

    try:
        print("📥 Incoming event:", json.dumps(event, indent=2))

        # --- Required fields ---
        user_id = event["userId"]
        cluster_id = event["clusterId"]
        file_id = event["fileId"]
        credit_id = event.get("creditId")
        job_id = event.get("jobId")

        filename = get_original_filename_from_mongo(file_id)

        bucket = event.get("bucket", S3_BUCKET_NAME)
        region = "ap-south-1"

        file_oid = ObjectId(file_id)
        credit_oid = ObjectId(credit_id) if credit_id else None

        s3_key = f"{user_id}/{cluster_id}/raw/{filename}"
        print(f"🔹 S3 Key: {s3_key}")

        # --- Run Chunkr (STRICT PAGE MODE) ---
        try:
            print("⚙️ Running Chunkr (strict page mode)...")
            extraction_result = run_chunkr(bucket, s3_key, file_id, region)
        except Exception as e:
            print("❌ Chunkr execution failed:", str(e))
            raise

        # --- Validate output ---
        if not extraction_result or not isinstance(extraction_result, dict):
            raise ValueError("Invalid Chunkr output")

        page_count = extraction_result.get("page_count", 0)
        pages = extraction_result.get("pages", {})

        if page_count == 0 or not pages:
            raise ValueError("Chunkr returned zero pages")

        print(f"✅ Chunkr success — {page_count} pages extracted")

        return {
            **event,
            "status": "success",
            "page_count": page_count,
            "pages": pages,   # ✅ page-wise chunks
            "filename": filename,
        }

    except Exception as e:
        print("❌ Lambda failure:", str(e))
        traceback.print_exc()

        mark_file_as_failed(event.get("fileId"))

        if credit_id and not credit_deleted:
            print(f"💳 Deleting credit {credit_id}")
            delete_credit_record(ObjectId(credit_id), ObjectId(file_id))
            credit_deleted = True

        update_job_status(
            event.get("jobId"),
            status="error",
            message=str(e)
        )

        return {
            **event,
            "status": "Failed",
            "page_count": page_count,
            "pages": None,
            "filename": event.get("fileName"),
        }
