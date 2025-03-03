import time
import requests

def wait_for_service(url, timeout=30):
    start_time = time.time()
    while True:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                print(f"Service is up and running at {url}")
                return
        except requests.exceptions.RequestException as e:
            print(f"Waiting for service at {url}... Error: {e}")

        if time.time() - start_time > timeout:
            raise TimeoutError(f"Service at {url} did not become available within {timeout} seconds")

        time.sleep(1)

if __name__ == "__main__":
    url = "http://app:8000"
    wait_for_service(url)