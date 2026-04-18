"""
dynamo_setup.py
Creates the single-table design for DynamoDB Local.
Run once before seeding: python setup/dynamo_setup.py
"""

import boto3
from botocore.exceptions import ClientError
import sys

ENDPOINT_URL = "http://localhost:8000"
TABLE_NAME   = "EcommerceDB"

def setup():
    try:
        dynamo = boto3.resource(
            "dynamodb",
            region_name="us-east-1",
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id="fake",
            aws_secret_access_key="fake",
        )
        print("[OK] Connected to DynamoDB Local")
    except Exception as e:
        print(f"[ERR] Could not connect to DynamoDB Local: {e}")
        sys.exit(1)

    # Drop existing table if present (idempotent re-run)
    try:
        existing = dynamo.Table(TABLE_NAME)
        existing.delete()
        existing.wait_until_not_exists()
        print(f"  Dropped existing table '{TABLE_NAME}'")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    table = dynamo.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    table.wait_until_exists()
    print(f"[OK] DynamoDB table '{TABLE_NAME}' created")
    print("  Key schema: PK (HASH) / SK (RANGE) - single-table design")
    print("  Access patterns:")
    print("    USER#<id>  / PROFILE       -> user record")
    print("    USER#<id>  / ORDER#<id>    -> order record")

if __name__ == "__main__":
    setup()
