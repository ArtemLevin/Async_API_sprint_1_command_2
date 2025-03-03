import time
import redis

if __name__ == '__main__':
    redis_client = redis.Redis(host='redis', port=6379, db=0)

    while True:
        try:
            if redis_client.ping():
                print("Redis is up and running!")
                break
        except redis.exceptions.ConnectionError as e:
            print(f"Redis is not yet available. Error: {e}")

        time.sleep(1)