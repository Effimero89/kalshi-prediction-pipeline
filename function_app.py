import azure.functions as func
import logging
import requests
import json
import os
import base64
import datetime
from azure.storage.blob import BlobServiceClient
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

app = func.FunctionApp()

KALSHI_BASE_URL = "https://api.elections.kalshi.com"

def get_signed_headers(method: str, path: str) -> dict:
    api_key = os.environ["KALSHI_API_KEY"]
    private_key_pem = os.environ["KALSHI_PRIVATE_KEY"].replace("\\n", "\n")
    timestamp = int(datetime.datetime.now().timestamp() * 1000)
    timestamp_str = str(timestamp)
    path_without_query = path.split("?")[0]
    msg_string = timestamp_str + method + path_without_query
    private_key = serialization.load_pem_private_key(private_key_pem.encode("utf-8"), password=None, backend=default_backend())
    signature = private_key.sign(msg_string.encode("utf-8"), padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
    sig_b64 = base64.b64encode(signature).decode("utf-8")
    return {"KALSHI-ACCESS-KEY": api_key, "KALSHI-ACCESS-TIMESTAMP": timestamp_str, "KALSHI-ACCESS-SIGNATURE": sig_b64, "Content-Type": "application/json"}

@app.timer_trigger(schedule="0 0 18 * * *", arg_name="kalshiTimer", run_on_startup=False)
def kalshi_ingest(kalshiTimer: func.TimerRequest) -> None:
    start_time = datetime.datetime.utcnow()
    total_markets = 0
    try:
        logging.info(f"kalshi_ingest started at {start_time}")
        connection_string = os.environ["ADLS_CONNECTION_STRING"]
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        container = blob_service.get_container_client("bronze")
        ingestion_date = datetime.datetime.utcnow().strftime("%Y/%m/%d")
        cursor = None
        path = "/trade-api/v2/markets"
        page_num = 0
        while True:
            params = {"limit": 1000, "status": "open"}
            if cursor:
                params["cursor"] = cursor
            headers = get_signed_headers("GET", path)
            response = requests.get(KALSHI_BASE_URL + path, headers=headers, params=params)
            if response.status_code != 200:
                logging.error(f"Kalshi API error: {response.status_code} {response.text}")
                break
            data = response.json()
            batch = data.get("markets", [])
            if not batch:
                break
            blob_path = f"{ingestion_date}/page_{page_num:04d}.json"
            blob_client = container.get_blob_client(blob_path)
            blob_client.upload_blob(json.dumps(batch), overwrite=True)
            total_markets += len(batch)
            page_num += 1
            logging.info(f"Wrote page {page_num}, total so far: {total_markets}")
            cursor = data.get("cursor")
            if not cursor:
                break
        logging.info(f"Ingestion complete. Total markets: {total_markets}, pages: {page_num}")
    except Exception as e:
        logging.error(f"kalshi_ingest failed: {str(e)}")
        raise
