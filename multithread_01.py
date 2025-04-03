# Multithreading: I/O-Bound Task Example
import threading
import requests

def download_file(url):
    response = requests.get(url)
    print(f"Downloaded {url}: {len(response.content)} bytes")

urls = [
    "https://example.com/file1",
    "https://example.com/file2",
    "https://example.com/file3"
]

threads = []
for url in urls:
    thread = threading.Thread(target=download_file, args=(url,))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()
# This script downloads files from multiple URLs concurrently using threads.