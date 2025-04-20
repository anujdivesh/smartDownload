import multiprocessing
import time
import os
import sys
from datetime import datetime, timedelta

# Add tasks directory to Python path
current_directory = os.path.dirname(os.path.abspath(__file__))
tasks_code_path = os.path.join(current_directory, 'tasks')
sys.path.append(tasks_code_path)

from task_scheduler import TaskScheduler
from Task_12_bom_forecast_hourly_ww3 import bom_forecast_hourly_ww3 as Task12

if __name__ == "__main__":
    # Configuration
    MAX_ATTEMPTS = 3
    ATTEMPT_DELAY = 10
    
    # Create and populate the task queue
    task_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()
    task_queue.put(Task12)
    print(f"Added {Task12.__name__} to the task queue")

    def worker():
        """Simplified worker that ensures task execution"""
        while True:
            try:
                task = task_queue.get_nowait()
                print(f"Worker starting task: {task.__name__}")
                try:
                    result = task()
                    result_queue.put(f"{task.__name__} completed successfully")
                except Exception as e:
                    result_queue.put(f"{task.__name__} failed: {str(e)}")
            except:
                break  # Queue is empty

    # Start worker process
    print("Starting worker process...")
    p = multiprocessing.Process(target=worker)
    p.start()
    
    # Wait with timeout
    p.join(timeout=300)  # 5 minute timeout
    
    # Check if process is still alive
    if p.is_alive():
        print("Task is taking too long, terminating...")
        p.terminate()
        p.join()
    
    # Get results
    while not result_queue.empty():
        print(result_queue.get())

    print("Task processing completed")
