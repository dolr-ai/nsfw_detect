import os
import json
from PIL import Image
from google.cloud import vision
from google.oauth2 import service_account
import logging

_LOGGER = logging.getLogger(__name__)

class NSFWDetect:
    def __init__(self, pipe3c, pipe5c):
        self.pipe3c = pipe3c
        self.pipe5c = pipe5c
        service_cred = os.environ.get("SERVICE_CRED")
        service_acc_creds = json.loads(service_cred)
        credentials = service_account.Credentials.from_service_account_info(service_acc_creds)
        self.gclient = vision.ImageAnnotatorClient(credentials=credentials)

    def nsfw_detect(self, imgs):
        marks = {'QUESTIONABLE provocative': 'provocative',
        'QUESTIONABLE porn': 'explicit',
        'QUESTIONABLE neutral': 'neutral',
        'QUESTIONABLE hentai': 'explicit',
        'QUESTIONABLE drawings': 'provocative',
        'UNSAFE provocative': 'nudity',
        'UNSAFE porn': 'explicit',
        'UNSAFE neutral': 'provocative',
        'UNSAFE hentai': 'explicit',
        'UNSAFE drawings': 'nudity',
        'SAFE provocative': 'provocative',
        'SAFE porn': 'explicit',
        'SAFE neutral': 'neutral',
        'SAFE hentai': 'explicit',
        'SAFE drawings': 'neutral'}
        
        from concurrent.futures import ThreadPoolExecutor

        res3cs = self.pipe3c(imgs)
        res5cs = self.pipe5c(imgs)

        mark3cs = [max(res3c, key=lambda x: x['score']) for res3c in res3cs]
        mark5cs = [max(res5c, key=lambda x: x['score']) for res5c in res5cs] 

        marks = [(marks[mark3c['label'] + ' ' + mark5c['label']], mark3c['score'], mark5c['score']) for mark3c, mark5c in zip(mark3cs, mark5cs)]
        return marks

    def detect_nsfw_gore(self, pil_image):
        try:
            """Detects NSFW content in a PIL image and returns the safe search annotation."""

            # Convert PIL image to bytes
            import io
            img_byte_arr = io.BytesIO()
            pil_image.save(img_byte_arr, format='PNG')
            content = img_byte_arr.getvalue()

            image = vision.Image(content=content)

            # Perform safe search detection
            response = self.gclient.safe_search_detection(image=image)
            safe = response.safe_search_annotation

            # Define likelihood names
            likelihood_name = (
                "UNKNOWN",
                "VERY_UNLIKELY",
                "UNLIKELY",
                "POSSIBLE",
                "LIKELY",
                "VERY_LIKELY",
            )

            # Create a dictionary with the results
            result = {
                "adult": likelihood_name[safe.adult],
                "medical": likelihood_name[safe.medical],
                "spoofed": likelihood_name[safe.spoof],
                "violence": likelihood_name[safe.violence],
                "racy": likelihood_name[safe.racy]
            }

            if response.error.message:
                raise Exception(
                    "{}\nFor more info on error messages, check: "
                    "https://cloud.google.com/apis/design/errors".format(response.error.message)
                )

            return result['violence']
        except Exception as e:
            _LOGGER.error(f"Error detecting NSFW content: {e}")
            return "UNKNOWN"

if __name__ == "__main__":
    import torch
    import pickle
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
    pipe3c, pipe5c = load_model_artifacts("model_artifacts")
    detector = NSFWDetect(pipe3c, pipe5c)
    img = Image.open("/Users/jaydhanwant/Downloads/WhatsApp Image 2024-08-29 at 13.18.09.jpeg")
    img2 = Image.open("/Users/jaydhanwant/Downloads/3.jpg")
    img3 = Image.open("/Users/jaydhanwant/Documents/questionable.png")
    imgs = [img, img2, img3]
    result = detector.nsfw_detect(imgs) 
    print(result)
    