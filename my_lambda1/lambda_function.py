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
    """
    Fetch original filename from MongoDB using file_id.
    """
    collection = get_mongo_collection("tb_file_details")
    doc = collection.find_one({"_id": ObjectId(file_id)})

    if not doc:
        raise ValueError(f"No file found in MongoDB for file_id: {file_id}")

    return doc.get("originalS3File")


def lambda_handler(event, context):
    """
    Run Chunkr text extraction (Mongo-safe orchestration).
    """

    # -----------------------------
    # SAFE DEFAULTS (CRITICAL)
    # -----------------------------
    page_count = 0
    filename = None
    extraction_result = None
    credit_id = None
    credit_oid = None
    file_oid = None

    try:
        print("📥 Incoming event:", json.dumps(event, indent=2))

        # --- Required fields ---
        user_id = event["userId"]
        cluster_id = event["clusterId"]
        file_id = event["fileId"]
        credit_id = event.get("creditId")
        job_id = event.get("jobId")

        filename = get_original_filename_from_mongo(file_id)

        region = "ap-south-1"
        bucket = event.get("bucket", S3_BUCKET_NAME)

        file_oid = ObjectId(file_id)
        credit_oid = ObjectId(credit_id) if credit_id else None

        # --- Build S3 key ---
        s3_key = f"{user_id}/{cluster_id}/raw/{filename}"
        print(f"🔹 S3 Key: {s3_key}")

        # -----------------------------
        # Run Chunkr
        # -----------------------------
        try:
            print("⚙️ Running Chunkr extraction...")
            extraction_result = run_chunkr(bucket, s3_key, file_id, region)
        except Exception as e:
            print("❌ Exception during run_chunkr:", str(e))
            traceback.print_exc()

            # idempotent credit cleanup
            if credit_oid:
                try:
                    delete_credit_record(credit_oid, file_oid)
                except Exception as ce:
                    print(f"⚠️ Credit delete skipped: {ce}")

            mark_file_as_failed(file_id)
            update_job_status(job_id, status="error", message=str(e))

            return {
                **event,
                "status": "Failed",
                "creditId": credit_id,
                "clusterId": cluster_id,
                "userId": user_id,
                "fileId": file_id,
                "filename": filename,
                "page_count": page_count,
                "normalized_data": None,
            }

        # -----------------------------
        # Validate output
        # -----------------------------
        if not extraction_result or not isinstance(extraction_result, dict):
            print("❌ Invalid Chunkr output")

            if credit_oid:
                try:
                    delete_credit_record(credit_oid, file_oid)
                except Exception as ce:
                    print(f"⚠️ Credit delete skipped: {ce}")

            mark_file_as_failed(file_id)
            update_job_status(job_id, status="error", message="Chunkr extraction failed")

            return {
                **event,
                "status": "Failed",
                "creditId": credit_id,
                "clusterId": cluster_id,
                "userId": user_id,
                "fileId": file_id,
                "filename": filename,
                "page_count": page_count,
                "normalized_data": None,
            }

        # -----------------------------
        # Success path
        # -----------------------------
        page_count = extraction_result.get("page_count", 0)
        normalized_data = extraction_result.get("normalized_data")

        print(f"✅ Chunkr completed successfully — {page_count} pages processed.")

        return {
            **event,
            "status": "success",
            "creditId": credit_id,
            "clusterId": cluster_id,
            "userId": user_id,
            "fileId": file_id,
            "filename": filename,
            "page_count": page_count,
            "normalized_data": normalized_data,
        }

    except Exception as e:
        print("❌ Fatal Lambda error:", str(e))
        traceback.print_exc()

        mark_file_as_failed(event.get("fileId"))

        if credit_oid:
            try:
                delete_credit_record(credit_oid, file_oid)
            except Exception as ce:
                print(f"⚠️ Credit delete skipped: {ce}")

        update_job_status(event.get("jobId"), status="error", message=str(e))

        return {
            **event,
            "status": "Failed",
            "creditId": credit_id,
            "clusterId": cluster_id,
            "userId": event.get("userId"),
            "fileId": event.get("fileId"),
            "filename": filename,
            "page_count": page_count,
            "normalized_data": None,
        }
