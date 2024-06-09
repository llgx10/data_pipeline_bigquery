from functools import wraps
import json
import random
import time
import traceback
def handle_errors(max_retries=3, base_delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    result = func(*args, **kwargs)
                    return result  # Return the result if no exception occurred
                except FileNotFoundError:
                    print("Credentials file not found.")
                except json.JSONDecodeError:
                    print("Error decoding JSON in the credentials file.")
                except KeyError as keyerror:
                    print(f"Error with key: {keyerror}")
                except Exception as e:
                    traceback.print_exc()
                    print("An unexpected error occurred:", str(e))
                
                # Exponential backoff retry logic
                delay = (2 ** retries) * base_delay
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay + random.uniform(0, 1))
                retries += 1
            
            print("Max retries exceeded. Exiting.")
            return None  # Return None if max retries exceeded
        return wrapper
    
    # Check if the decorator was called with arguments or not
    if callable(max_retries):
        func = max_retries
        max_retries = 3
        base_delay = 1
        return decorator(func)
    else:
        return decorator