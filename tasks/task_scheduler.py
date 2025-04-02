import multiprocessing
import time
import psutil
import gc
from multiprocessing.queues import Empty
import psutil
import signal
import os
class TaskScheduler:
    @staticmethod
    def get_available_cpus(utilization_threshold=80):
        cpu_percent = psutil.cpu_percent(interval=1, percpu=True)
        free_cpus = [i for i, percent in enumerate(cpu_percent) if percent < utilization_threshold]
        return free_cpus

    @staticmethod
    def is_memory_available(memory_threshold=80):
        memory_info = psutil.virtual_memory()
        memory_percent_used = memory_info.percent
        print(f"Memory Usage: {memory_percent_used}%")
        return memory_percent_used < memory_threshold
    """
    @staticmethod
    def worker(task_queue, result_queue, cpu_id):
        psutil.Process().cpu_affinity([cpu_id])
        while not task_queue.empty():
            try:
                func = task_queue.get()
                print(f"Running {func.__name__} on CPU {cpu_id}")
                result = func()
                result_queue.put(result)
            except Exception as e:
                result_queue.put(f"Error in task: {e}")
            finally:
                task_queue.task_done()
    """
    @staticmethod
    def worker(task_queue, result_queue, cpu_id):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        
        psutil.Process().cpu_affinity([cpu_id])
        while True:
            try:
                func = task_queue.get_nowait()
            except Empty:
                break  # Exit if queue is empty

            try:
                print(f"🚀 Running {func.__name__} on CPU {cpu_id} (PID: {os.getpid()})")
                result = func()
                result_queue.put((func.__name__, result))
            except Exception as e:
                print(f"❌ Task {func.__name__} failed: {str(e)}")
                result_queue.put((func.__name__, f"Error: {str(e)}"))
            finally:
                task_queue.task_done()