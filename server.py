import pickle
from concurrent import futures
import contextlib
import datetime
import logging
import multiprocessing
import socket
import sys
import time
import os
from typing import Any
import jwt
import consts
import grpc
from grpc_reflection.v1alpha import reflection
import nsfw_detector_pb2
import nsfw_detector_pb2_grpc
from utils.gcs_utils import get_images_from_gcs
from nsfw_detect_utils import NSFWDetect
from save_model_artifacts import download_artifacts # hardocded the gcr paths here TODO: Move that to a config
import torch 


_LOGGER = logging.getLogger(__name__)

_ONE_DAY = datetime.timedelta(days=1)
_PROCESS_COUNT = multiprocessing.cpu_count()
_LOGGER.info(f"Process count: {_PROCESS_COUNT}")
# _PROCESS_COUNT = 1 
_THREAD_CONCURRENCY = 10 # heuristic
_BIND_ADDRESS = "[::]:50051"


_AUTH_HEADER_KEY = "authorization"

_PUBLIC_KEY = consts.NSFW_JWT_PUB_KEY
_JWT_PAYLOAD = {
    "sub": "yral-nsfw-detector-server",
    "company": "gobazzinga",
}

# downloading model artifacts 
artifact_path = "model_artifacts"
artifact_files = ['pipe3c.pkl', 'pipe5c.pkl']
missing_files = [file for file in artifact_files if not os.path.exists(os.path.join(artifact_path, file))]

if missing_files:
    download_artifacts()



def load_model_artifacts(artifact_path):
    print("Loading model artifacts")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    with open(artifact_path + '/pipe3c.pkl', "rb") as f:
        pipe3c = pickle.load(f)
    with open(artifact_path + '/pipe5c.pkl', "rb") as f:
        pipe5c = pickle.load(f)

    pipe3c.device = device
    pipe5c.device = device
    
    return pipe3c, pipe5c

class SignatureValidationInterceptor(grpc.ServerInterceptor):
    def __init__(self):
        def abort(ignored_request, context):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid signature")

        self._abort_handler = grpc.unary_unary_rpc_method_handler(abort)

    def intercept_service(self, continuation, handler_call_details):
        metadata_dict = dict(handler_call_details.invocation_metadata)
        token = metadata_dict[_AUTH_HEADER_KEY].split()[1]
        payload = jwt.decode(
            token,
            _PUBLIC_KEY,
            algorithms=["EdDSA"],
        )

        if payload == _JWT_PAYLOAD:
            return continuation(handler_call_details)
        else:
            print(f"Received payload: {payload}")
            return self._abort_handler


class NSFWDetectorServicer(nsfw_detector_pb2_grpc.NSFWDetectorServicer):
    def __init__(self):
        self.pipe3c, self.pipe5c = load_model_artifacts("model_artifacts")
        self.nsfw_detector = NSFWDetect(self.pipe3c, self.pipe5c)
        _LOGGER.info("Loaded models: ")
        _LOGGER.info(self.pipe3c.model.config) 
        _LOGGER.info(self.pipe5c.model.config)
        _LOGGER.info('='*100)

    def DetectNSFW(self, request, context):
        _LOGGER.info("Request received")
        video_id = request.video_id
        nsfw_tag, gore_tag = self.process_frames(video_id)
        response = nsfw_detector_pb2.NSFWDetectorResponse(nsfw_ec=nsfw_tag, nsfw_gore=gore_tag)
        return response

    def process_frames(self, video_id):
        frames = get_images_from_gcs("yral-video-frames", video_id)
        nsfw_tags = []
        gore_tags = []
        for frame in frames:
            nsfw_tags.append(self.nsfw_detector.nsfw_detect(frame['image'])['res'])
            gore_tags.append(self.nsfw_detector.detect_nsfw_gore(frame['image']))
        tag_priority = "explicit nudity provocative neutral".split()
        gore_priority = ["UNKNOWN", "VERY_UNLIKELY", "UNLIKELY", "POSSIBLE", "LIKELY", "VERY_LIKELY"][::-1]
        # Sort nsfw_tags based on the priority defined in tag_priority
        nsfw_tags.sort(key=lambda tag: tag_priority.index(tag))
        gore_tags.sort(key=lambda tag: gore_priority.index(tag))

        nsfw_tag = None
        gore_tag = None
        if len(nsfw_tags) > 0:
            nsfw_tag = nsfw_tags[0]
        if len(gore_tags) > 0:
            gore_tag = gore_tags[0]
            
        return [nsfw_tag, gore_tag]


        

def _wait_forever(server):
    try:
        while True:
            time.sleep(_ONE_DAY.total_seconds())
    except KeyboardInterrupt:
        server.stop(None)

def _run_server():
    _LOGGER.info("Starting new server.")
    options = (("grpc.so_reuseport", 1),)

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=_THREAD_CONCURRENCY),
        interceptors=(SignatureValidationInterceptor(),),
        options=options,
    )
    nsfw_detector_pb2_grpc.add_NSFWDetectorServicer_to_server(
        NSFWDetectorServicer(), server
    )
    SERVICE_NAMES = (
        nsfw_detector_pb2.DESCRIPTOR.services_by_name['NSFWDetector'].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)
    server.add_insecure_port(_BIND_ADDRESS)
    server.start()
    _LOGGER.info(f"Server started on {_BIND_ADDRESS}")
    _wait_forever(server)

def main():
    multiprocessing.set_start_method("spawn", force=True)
    _LOGGER.info(f"Binding to '{_BIND_ADDRESS}'")
    sys.stdout.flush()
    workers = []
    for _ in range(_PROCESS_COUNT):
        worker = multiprocessing.Process(target=_run_server)
        worker.start()
        workers.append(worker)
    for worker in workers:
        worker.join()

if __name__ == "__main__":
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("[PID %(process)d] %(message)s")
    handler.setFormatter(formatter)
    _LOGGER.addHandler(handler)
    _LOGGER.setLevel(logging.INFO)
    main()