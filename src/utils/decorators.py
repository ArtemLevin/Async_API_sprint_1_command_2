from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

def with_retry():
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )