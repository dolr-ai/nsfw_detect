NSFW_JWT_PUB_KEY = "-----BEGIN PUBLIC KEY-----\
MCowBQYDK2VwAyEA9zi53RuUKNEgopKZPOycJFaPdaBBG7WDWB8OSaz/jWM=\
-----END PUBLIC KEY-----"

# NSFW_JWT_PUB_KEY_PROD = "-----BEGIN PUBLIC KEY-----\
# MCowBQYDK2VwAyEA7e47Br+mHdlETtxFlmsHn2xv8bH16tF1oLPfnL0jgSY=\
# -----END PUBLIC KEY-----"

PROJECT_NAME = "hot-or-not-feed-intelligence"
STAGE_PROJECT_NAME = "jay-dhanwant-experiments"
EMBEDDING_DIM = 1408
DATASET_NAME = "yral_ds"
STAGE_DATASET_NAME = "stage_tables"

# BigQuery Tables
use_stage = True
if use_stage:
    VIDEO_EMBEDDINGS_TABLE = f"{STAGE_PROJECT_NAME}.{STAGE_DATASET_NAME}.stage_video_embeddings"
    MODEL_ARTIFACTS_BUCKET = "stage-yral-ds-model-artifacts"
    PROJECT_NAME = STAGE_PROJECT_NAME
else:
    VIDEO_EMBEDDINGS_TABLE = f"{PROJECT_NAME}.{DATASET_NAME}.video_embeddings"
    MODEL_ARTIFACTS_BUCKET = "yral-ds-model-artifacts"

# Add after the existing constants
MODEL_ARTIFACTS_FILES = [
    'pipe3c.pkl',
    'pipe5c.pkl',
    'nsfw_rf_classifier_40k.pkl'
]

# GCS Buckets
VIDEO_FRAMES_BUCKET = "yral-video-frames"
if use_stage:
    VIDEO_FRAMES_BUCKET = "stage-yral-video-frames"

# Default parameters
DEFAULT_GCS_WORKERS = 20

