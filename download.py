import requests

def download_file(url: str, file_name: str):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(f"{file_name}", 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
    return response.status_code