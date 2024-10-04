import requests
import urllib.request
from bs4 import BeautifulSoup
from collections import deque
from html.parser import HTMLParser
from urllib.parse import urlparse, urljoin
import os
import ssl
import hashlib

ssl._create_default_https_context = ssl._create_unverified_context

# Define root domain to crawl
domain = "sfbu.edu"
full_url = "https://www.sfbu.edu/"

# Create a class to parse the HTML and get the hyperlinks
class HyperlinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hyperlinks = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "a" and "href" in attrs:
            self.hyperlinks.append(attrs["href"])

# Function to get the hyperlinks from a URL
def get_hyperlinks(url):
    try:
        with urllib.request.urlopen(url) as response:
            if not response.info().get('Content-Type').startswith("text/html"):
                return []
            html = response.read().decode('utf-8')
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return []

    parser = HyperlinkParser()
    parser.feed(html)

    return parser.hyperlinks or []  # Ensure an empty list is returned if parsing fails

# Function to get domain-specific hyperlinks, and convert relative URLs to absolute URLs
def get_domain_hyperlinks(local_domain, base_url, url):
    hyperlinks = get_hyperlinks(url)
    return [urljoin(base_url, link) if link.startswith('/') else link for link in hyperlinks if link and (local_domain in link or link.startswith('/'))]

# Helper function to create a short, unique filename using MD5 hash
def get_safe_filename(url):
    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
    return url_hash

def crawl(url):
    local_domain = urlparse(url).netloc
    base_url = f"https://{local_domain}"
    queue = deque([url])
    seen = set([url])

    if not os.path.exists(f"text/{local_domain}"):
        os.makedirs(f"text/{local_domain}")

    while queue:
        url = queue.pop()
        print(f"Crawling: {url}")
        try:
            response = requests.get(url)
            if "text/html" in response.headers.get("Content-Type", ""):
                soup = BeautifulSoup(response.text, "lxml")
                text = soup.get_text()

                # Use a short hash for filenames to avoid filename length issues
                filename = get_safe_filename(url)
                with open(f"text/{local_domain}/{filename}.txt", "w", encoding="utf-8") as f:
                    f.write(text)

                for link in get_domain_hyperlinks(local_domain, base_url, url):
                    if link not in seen:
                        queue.append(link)
                        seen.add(link)
        except requests.exceptions.RequestException as e:
            print(f"Failed to crawl {url}: {e}")

if __name__ == "__main__":
    crawl(full_url)
     
