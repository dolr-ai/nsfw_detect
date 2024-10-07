class NSFWDetect:
    def __init__(self, pipe3c, pipe5c):
        self.pipe3c = pipe3c
        self.pipe5c = pipe5c

    def nsfw_detect(self, img):
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

        res3c = self.pipe3c(img)
        res5c = self.pipe5c(img)

        mark3c = max(res3c, key=lambda x: x['score'])['label']
        mark5c = max(res5c, key=lambda x: x['score'])['label'] 

        mark = marks[mark3c + ' ' + mark5c]
        return {'res':mark, 'metadata': {'mark3c': mark3c, 'mark5c': mark5c}}
