import pandas as pd
import re


def preprocess_text(text):
    if pd.isna(text):
        return ""
    
    text = str(text).lower() 
    
    text = re.sub(r'[\s,:]+', ' ', text)
    
    text = re.sub(r'[\r\n\t\-]+', ' ', text)
    
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text


if __name__ == "__main__" :
    dataset = pd.read_csv("data/mtsamples.csv")
    dataset['clean_transcription'] = dataset['transcription'].apply(preprocess_text)