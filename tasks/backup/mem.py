import multiprocessing
import time
import psutil  # For CPU, memory, and process tracking
import gc  # For garbage collection

# Define your functions
def function_1():
    print("function 1 started (memory-intensive)")
    # Simulate memory-intensive task by allocating a large list
    large_list = [0] * (10 ** 8)  # Allocate ~800 MB of memory (10^8 integers)
    time.sleep(10)  # Simulate work
    del large_list  # Free the memory
    gc.collect()  # Force garbage collection
    print("function 1 completed (memory freed)")
    return "Function 1 completed"

def function_2():
    print("function 2 started")
    time.sleep(5)  # Simulate work
    return "Function 2 completed"

def function_3():
    print("function 3 started")
    time.sleep(8)  # Simulate work
    return "Function 3 completed"

def function_4():
    print("function 4 started")
    time.sleep(3)  # Simulate work
    return "Function 4 completed"

def function_5():
    print("function 5 started")
    time.sleep(2)  # Simulate work
    return "Function 5 completed"

def function_6():
    print("function 6 started")
    time.sleep(1)  # Simulate work
    return "Function 6 completed"

# Function to get available CPUs based on utilization
def get_available_cpus(utilization_threshold=60):
    cpu_percent = psutil.cpu_percent(interval=1, percpu=True)
    free_cpus = [i for i, percent in enumerate(cpu_percent) if percent < utilization_threshold]
    return free_cpus

# Function to check memory usage
def is_memory_available(memory_threshold=80):
    memory_info = psutil.virtual_memory()
    memory_percent_used = memory_info.percent
    print(f"Memory Usage: {memory_percent_used}%")
    return memory_percent_used < memory_threshold

# Worker function to execute tasks
def worker(task_queue, result_queue, cpu_id):
    # Pin the process to the specified CPU
    psutil.Process().cpu_affinity([cpu_id])
    while not task_queue.empty():
        try:
            func = task_queue.get()
            print(f"Running {func.__name__} on CPU {cpu_id}")
            result = func()
            result_queue.put(result)
        except Exception as e:
            result_queue.put(f"Error in task: {e}")

if __name__ == "__main__":
    # List of functions to execute
    functions = [function_1, function_2, function_3, function_4, function_5, function_6]

    # Create a task queue and a result queue
    task_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()

    # Add all functions to the task queue
    for func in functions:
        task_queue.put(func)

    # Create a list to hold active processes
    processes = []

    # Continuously check CPU utilization and memory usage, and distribute tasks
    while not task_queue.empty():
        if not is_memory_available(memory_threshold=80):
            print("Memory usage is high. Waiting...")
            time.sleep(10)  # Wait for memory usage to drop
            continue

        # Check CPU utilization
        free_cpus = get_available_cpus(utilization_threshold=60)
        print(f"Free CPUs: {free_cpus}")

        # Start a worker process for each free CPU
        for cpu_id in free_cpus:
            if not task_queue.empty():
                p = multiprocessing.Process(target=worker, args=(task_queue, result_queue, cpu_id))
                p.start()
                processes.append(p)
                time.sleep(0.1)  # Small delay to avoid race conditions

        # Wait a bit before rechecking CPU utilization and memory usage
        time.sleep(1)

    # Wait for all processes to complete
    for p in processes:
        p.join()

    # Collect and print results
    while not result_queue.empty():
        print(result_queue.get())