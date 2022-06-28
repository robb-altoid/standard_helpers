from googletrans import Translator
import time

def trans_text(input_text:str):
    """
    Uses the googletrans package to send text to the Google Translation API. The function uses a randomly generated
    amount of time between 2 and 5 seconds to be sure not overwhelm the speedlimit on the api. Need to have PIP
    installed googletrans 4.0.0rc1 in your environment before importing this module (pip install googletrans==4.0.0rc1).
    Uses Google's automatic language detection to return the translation.

    args
        input_text:str 
            the text to be translated

    outputs
        translated_text:str
            the translated text
    """
    translator = Translator()
    time.sleep(random.randrange(2, 5))  # attempt to resolve the HTTP 429 error - too many requests
    try:
        translated_text = translator.translate(text, dest='en')  # let Google Translate determine the input language so it will work on many languages -> English output
        return translated_text.text
    except:
        translated_text = 'nothing returned from translation function'
        return translated_text