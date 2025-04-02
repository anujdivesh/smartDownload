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
from Task_13_cmems_forecast_daily_salinity import cmems_forecast_daily_salinity as Task13
from Task_14_cmems_forecast_daily_phytoplankton import cmems_forecast_daily_phytoplankton as Task14
from Task_15_cmems_hindcast_monthly_phytoplankton import cmems_hindcast_daily_phytoplankton as Task15
from Task_16_cmems_forecast_daily_ph import cmems_forecast_daily_ph as Task16
from Task_17_cmems_hindcast_monthly_ph import cmems_hindcast_monthly_ph as Task17
from Task_18_noaa_nrt_daily_mhw import noaa_nrt_daily_mhw as Task18
from multiprocessing import Queue, Process, JoinableQueue
import os

if __name__ == "__main__":
    # List of functions to execute
    functions = [Task1, Task2, Task3, Task4, Task5, Task6, Task7, Task8, Task9, Task10, Task11, Task12,\
                 Task13, Task14,Task15,Task16, Task17,Task18]

    # Create a task queue and a result queue
    task_queue = JoinableQueue()
    result_queue = Queue()

    # Add tasks to queue
    for func in functions:
        task_queue.put(func)
        print(f"📥 Added {func.__name__} to the queue")

    # Start workers
    processes = []
    try:
        while not task_queue.empty():
            free_cpus = TaskScheduler.get_available_cpus(utilization_threshold=60)
            for cpu_id in free_cpus:
                if task_queue.empty():
                    break

                p = Process(
                    target=TaskScheduler.worker,
                    args=(task_queue, result_queue, cpu_id),
                    daemon=False  # Critical: Ensures process won't terminate abruptly
                )
                p.start()
                processes.append(p)
                time.sleep(0.1)  # Avoid overloading CPU

            time.sleep(5)  # Longer delay between checks (reduces CPU overhead)

        # Block until all tasks are done
        task_queue.join()

    except KeyboardInterrupt:
        print("\n⚠️ Received interrupt. Waiting for running tasks to finish...")
    finally:
        # Ensure all processes terminate
        for p in processes:
            if p.is_alive():
                p.join(timeout=30)  # Wait 30 sec for graceful exit

        # Print results
        print("\n📊 Results:")
        while not result_queue.empty():
            task_name, result = result_queue.get()
            status = "✅ SUCCESS" if not str(result).startswith("Error:") else "❌ FAILED"
            print(f"{task_name}: {status}")