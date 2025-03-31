# %%
import os 
import requests
import pickle
from google.cloud import storage
from google.oauth2 import service_account
import json
import consts

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your GCS object
    # source_blob_name = "storage-object-name"

    # The path to which the file should be downloaded
    # destination_file_name = "local/path/to/file"

    print(f"Downloading {source_blob_name} from {bucket_name} to {destination_file_name}")
    
    service_cred = os.environ.get("SERVICE_CRED")
    service_acc_creds = json.loads(service_cred)
    credentials = service_account.Credentials.from_service_account_info(service_acc_creds)
    storage_client = storage.Client(credentials=credentials, project=consts.PROJECT_NAME)

    bucket = storage_client.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )

def download_artifacts():
    # Create the directory if it doesn't exist
    print("Downloading model artifacts")
    os.makedirs("model_artifacts", exist_ok=True)

    # Download the files if they don't already exist
    for artifact_file in consts.MODEL_ARTIFACTS_FILES:
        if not os.path.exists(f"model_artifacts/{artifact_file}"):
            download_blob(
                consts.MODEL_ARTIFACTS_BUCKET, 
                artifact_file, 
                f"model_artifacts/{artifact_file}"
            )

    # Load the models
    pipe3c = None
    pipe5c = None
    nsfw_rf_classifier_40k = None

    for artifact_file in consts.MODEL_ARTIFACTS_FILES:
        with open(f"model_artifacts/{artifact_file}", "rb") as f:
            if artifact_file == "pipe3c.pkl":
                pipe3c = pickle.load(f)
            elif artifact_file == "pipe5c.pkl":
                pipe5c = pickle.load(f)
            elif artifact_file == "nsfw_rf_classifier_40k.pkl":
                nsfw_rf_classifier_40k = pickle.load(f)

    print(pipe3c.model.config)  
    print(pipe5c.model.config)  

    return pipe3c, pipe5c, nsfw_rf_classifier_40k


