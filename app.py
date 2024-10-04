from flask import Flask, render_template, request, jsonify
import subprocess
import os

app = Flask(__name__)

# Route to display the front-end (index.html)
@app.route('/')
def index():
    return render_template('index.html')

# Step 1: Start Crawling
@app.route('/start_crawling', methods=['POST'])
def start_crawling():
    domain = request.form['domain']
    url = request.form['url']
    file_path = request.form['file_path']
    crawl_limit = request.form['crawl_limit']

    try:
        # Call the crawler.py script as a subprocess with arguments
        process = subprocess.Popen(
            ['python3', 'crawler.py', domain, url, file_path, crawl_limit],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()

        # Capture the output from the crawler
        if stderr:
            output = stderr.decode('utf-8')
        else:
            output = stdout.decode('utf-8')

        # Pass the output to the HTML to display
        return render_template('index.html', crawler_output=output)

    except Exception as e:
        return str(e)

# Step 2: Generate Embeddings
@app.route('/generate_embeddings', methods=['POST'])
def generate_embeddings():
    try:
        # Call the embedding.py script as a subprocess
        process = subprocess.Popen(
            ['python3', 'embedding.py'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()

        # Capture the output from the embedding process
        if stderr:
            output = stderr.decode('utf-8')
        else:
            output = stdout.decode('utf-8')

        # Pass the output to the HTML to display
        return render_template('index.html', embedding_output=output)

    except Exception as e:
        return str(e)

# Step 3: Ask Questions
@app.route('/ask_question', methods=['POST'])
def ask_question():
    question = request.form['question']

    try:
        # Call the logic from commandline_app.py to answer the question
        from commandline_app import answer_question

        # Get the answer to the question
        answer = answer_question(question)

        # Pass the answer to the HTML to display
        return render_template('index.html', answer_output=answer)

    except Exception as e:
        return str(e)

if __name__ == '__main__':
    app.run(debug=True)