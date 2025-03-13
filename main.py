import multiprocessing
import time
import os
import sys

# Add tasks directory to Python path
current_directory = os.path.dirname(os.path.abspath(__file__))
tasks_code_path = os.path.join(current_directory, 'tasks')
sys.path.append(tasks_code_path)

from task_scheduler import TaskScheduler
# Import tasks
from Task_1_nasa_nrt_daily_cholorophyll import nasa_nrt_daily_choloropphyll as Task1
from Task_2_nasa_nrt_monthly_chlorophyll import nasa_nrt_monthly_choloropphyll as Task2
from Task_3_noaa_nrt_daily_coral_bleaching import noaa_nrt_daily_coral_bleaching as Task3
from Task_4_noaa_forecast_monthly_coral_bleaching import noaa_forecast_monthly_coral_bleaching as Task4
from Task_5_noaa_hindcast_daily_sst_anomalies import noaa_hindcast_daily_sst_anomalies as Task5
from Task_6_noaa_nrt_daily_sst_anomalies import noaa_nrt_daily_sst_anomalies as Task6
from Task_7_noaa_hindcast_monthly_ersst import noaa_nrt_daily_sst_anomalies as Task7
from Task_8_bom_forecast_monthly_sst import bom_forecast_monthly_sst as Task8
from Task_9_bom_forecast_monthly_ssh import bom_forecast_monthly_ssh as Task9
from Task_10_cmems_nrt_daily_ssh import cmems_nrt_daily_ssh as Task10
from Task_11_bom_forecast_seasonal_sst import bom_forecast_seasonal_sst as Task11
from Task_12_bom_forecast_hourly_ww3 import bom_forecast_hourly_ww3 as Task12

if __name__ == "__main__":
    # List of functions to execute
    functions = [Task1, Task2, Task3, Task4, Task5, Task6, Task7, Task8, Task9, Task10, Task11, Task12]

    # Create a task queue and a result queue
    task_queue = multiprocessing.JoinableQueue()
    result_queue = multiprocessing.Queue()

    # Add all functions to the task queue
    for func in functions:
        task_queue.put(func)
        print(f"Added {func.__name__} to the task queue")

    # Verify the queue is populated
    print(f"Task queue size: {task_queue.qsize()}")

    # Create a list to hold active processes
    processes = []

    # Continuously check CPU utilization and memory usage, and distribute tasks
    while not task_queue.empty():
        if not TaskScheduler.is_memory_available(memory_threshold=80):
            print("Memory usage is high. Waiting...")
            time.sleep(10)  # Wait for memory usage to drop
            continue

        # Check CPU utilization
        free_cpus = TaskScheduler.get_available_cpus(utilization_threshold=60)
        print(f"Free CPUs: {free_cpus}")

        # Start a worker process for each free CPU
        for cpu_id in free_cpus:
            if not task_queue.empty():
                p = multiprocessing.Process(target=TaskScheduler.worker, args=(task_queue, result_queue, cpu_id))
                p.start()
                processes.append(p)
                print(f"Started worker process on CPU {cpu_id}")
                time.sleep(0.1)  # Small delay to avoid race conditions

        # Wait a bit before rechecking CPU utilization and memory usage
        time.sleep(1)

    # Wait for all processes to complete
    for p in processes:
        p.join()

    # Collect and print results
    while not result_queue.empty():
        print(result_queue.get())