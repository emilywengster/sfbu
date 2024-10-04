import os
import openai
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# Batch size for sending multiple texts at once
BATCH_SIZE = 5  # Adjust this according to the number of text files

def get_embeddings_batch(texts):
    """Get OpenAI embeddings for a batch of texts."""
    response = openai.embeddings.create(input=texts, model="text-embedding-ada-002")
    return [item.embedding for item in response.data]

def generate_embeddings():
    """Generate embeddings for all text files and save them to a CSV."""
    domain = "www.sfbu.edu"  # Correct directory name
    text_folder = f"text/{domain}"
    
    # Check if the directory exists
    if not os.path.exists(text_folder):
        print(f"Directory {text_folder} not found.")
        return
    
    files = [os.path.join(text_folder, f) for f in os.listdir(text_folder) if f.endswith(".txt")]
    
    all_embeddings = []
    all_texts = []
    
    for i in range(0, len(files), BATCH_SIZE):
        batch_files = files[i:i + BATCH_SIZE]
        batch_texts = []

        for file in batch_files:
            with open(file, 'r', encoding='utf-8') as f:
                text = f.read().strip()
                if text:
                    batch_texts.append(text)
                    all_texts.append(text)

        if batch_texts:
            batch_embeddings = get_embeddings_batch(batch_texts)
            all_embeddings.extend(batch_embeddings)

    df = pd.DataFrame({"text": all_texts, "embeddings": all_embeddings})
    df.to_csv('processed/embeddings.csv', index=False)
    print("Embeddings generated and saved to 'processed/embeddings.csv'")

if __name__ == "__main__":
    generate_embeddings()
