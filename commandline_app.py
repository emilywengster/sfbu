import os
import openai
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sklearn.metrics.pairwise import cosine_similarity

# Load environment variables from .env file
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

def get_embedding(text):
    """Get OpenAI embedding for the given text."""
    response = openai.embeddings.create(input=[text], model="text-embedding-ada-002")
    return response.data[0].embedding

def create_context(question, df, max_len=1800):
    """Create context by finding the most similar text from the embeddings."""
    q_embedding = get_embedding(question)
    df['similarity'] = df['embeddings'].apply(lambda x: cosine_similarity([q_embedding], [np.array(eval(x))])[0][0])
    
    context = []
    cur_len = 0

    for i, row in df.sort_values(by="similarity", ascending=False).iterrows():
        cur_len += len(row['text'].split())
        if cur_len > max_len:
            break
        context.append(row['text'])

    return "\n\n###\n\n".join(context)

def answer_question(question):
    """Answer the question based on the most relevant context."""
    df = pd.read_csv('processed/embeddings.csv')
    context = create_context(question, df)

    # Constructing the messages for the chat completion
    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": f"Context: {context}\n\nQuestion: {question}\nAnswer:"}
    ]

    # Using the new method for OpenAI ChatCompletion
    response = openai.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=messages,
        temperature=0,
        max_tokens=150,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=["\n"]
    )
    
    return response.choices[0].message.content.strip()

if __name__ == "__main__":
    question = input("Ask a question: ")
    answer = answer_question(question)
    print(f"Answer: {answer}")