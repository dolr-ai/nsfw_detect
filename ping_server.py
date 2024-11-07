import grpc
import nsfw_detector_pb2
import nsfw_detector_pb2_grpc
import os
import jwt

token_fpath = "/Users/jaydhanwant/Documents/SS/nsfw_jwt_token.txt"
with open(token_fpath, 'r') as f:
    _JWT_TOKEN = f.read()

server_url = 'prod-yral-nsfw-classification.fly.dev:443'
# Load the private key from a path specified in an environment variable

def run():
    # NOTE: Replace with the actual server address if different
    channel = grpc.secure_channel(server_url, grpc.ssl_channel_credentials())
    stub = nsfw_detector_pb2_grpc.NSFWDetectorStub(channel)

    # Test video ID (replace with an actual video ID for testing)
    video_id = "00034f1c9c9148388bf6873776222535" 

    try:
        print("Sending request to server")
        metadata = [('authorization', f'Bearer {_JWT_TOKEN}')]
        response = stub.DetectNSFW(nsfw_detector_pb2.NSFWDetectorRequest(video_id=video_id), metadata=metadata)
        print(response)
    except grpc.RpcError as e:
        print(f"RPC failed: {e}")

if __name__ == '__main__':
    run()