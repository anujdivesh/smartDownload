import multiprocessing
import time
import psutil  # For CPU and process tracking

# Define your functions
def function_1():
    print("function 1 started")
    time.sleep(7200)
    return "Function 1 completed"

def function_2():
    print("function 2 started")
    time.sleep(7200)
    return "Function 2 completed"

def function_3():
    print("function 3 started")
    time.sleep(7200)
    return "Function 3 completed"

def function_4():
    print("function 4 started")
    time.sleep(10)
    return "Function 4 completed"

def function_5():
    print("function 5 started")
    time.sleep(2)
    return "Function 5 completed"

def function_6():
    print("function 6 started")
    time.sleep(5)
    return "Function 6 completed"

# Function to get available CPUs based on utilization
def get_available_cpus(utilization_threshold=50):
    # Get CPU utilization per core
    cpu_percent = psutil.cpu_percent(interval=1, percpu=True)

    # Debugging: Print CPU utilization values
    print(f"CPU Utilization: {cpu_percent}")

    # Determine which CPUs are free (utilization below threshold)
    free_cpus = []
    for i, percent in enumerate(cpu_percent):
        if percent is None:
            print(f"Warning: CPU {i} utilization is None. Assuming it's free.")
            free_cpus.append(i)
        elif percent < utilization_threshold:
            free_cpus.append(i)

    return len(free_cpus)

if __name__ == "__main__":
    # List of functions to execute
    functions = [function_1, function_2, function_3, function_4, function_5, function_6]

    # Get the number of available CPUs based on utilization
    num_workers = get_available_cpus(utilization_threshold=50)
    print(f"Available CPUs: {num_workers}")

    # Create a pool of workers
    with multiprocessing.Pool(processes=num_workers) as pool:
        # Use pool.apply_async to schedule the functions
        results = [pool.apply_async(func) for func in functions]

        # Wait for all results and print them as they complete
        for result in results:
            try:
                print(result.get())
            except Exception as e:
                print(f"Error in task: {e}")