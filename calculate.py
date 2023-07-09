# MB will be the smallest unit
from distutils.util import strtobool
import subprocess

def get_valid_input(message, f, error_message):
    amount = None
    while amount is None:
        try:
            amount = f(input(message))
            return amount
        except ValueError:
            print(error_message)

def get_jvm_max_mem(mem):
    command = ["java", f"-Xmx{int(mem)}m", "-cp", "calculate/target/calculate-1.0-SNAPSHOT.jar", "Helper"]
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    text = p.stdout.read()
    ret = p.wait()
    if ret == 0:
        return float(text.decode("utf-8"))

if __name__ == "__main__":
    RESERVED_SYSTEM_MEMORY = 300
    SPARK_MEMORY_FRACTION=0.6
    SPARK_MEMORY_STORAGEFRACTION=0.5
    GB_TO_MB_RATE = 1024
    spark_executor_memory = get_valid_input("Amount of spark.executor.memory (in GB): ", float, "Invalid value for spark.executor.memory, must be a number") * GB_TO_MB_RATE
    spark_executor_memory = get_jvm_max_mem(spark_executor_memory) / pow(GB_TO_MB_RATE, 2)
    spark_memory_offheap_enabled = get_valid_input("Option spark.memory.offHeap.enabled: ", strtobool, "Invalid value for spark.memory.offHeap.enabled, must be a boolean string (true, false, True, False,...)")
    if spark_memory_offheap_enabled:
        spark_memory_offheap_size = get_valid_input("Amount of spark.memory.offHeap.size (in GB): ", float, "Invalid value for spark.memory.offHeap.size, must be a number") * GB_TO_MB_RATE
    on_heap_user_memory = (spark_executor_memory - RESERVED_SYSTEM_MEMORY) * (1 - SPARK_MEMORY_FRACTION)
    on_heap_spark_memory = (spark_executor_memory - RESERVED_SYSTEM_MEMORY) * SPARK_MEMORY_FRACTION
    on_heap_spark_storage_memory = on_heap_spark_memory * SPARK_MEMORY_STORAGEFRACTION
    on_heap_spark_execution_memory = on_heap_spark_memory * (1 - SPARK_MEMORY_STORAGEFRACTION)
    total_spark_memory = on_heap_spark_memory
    print("\n")
    print(f"------------------ On-Heap Memory: {spark_executor_memory} MB ------------------")
    print(f"Researved Memory: {RESERVED_SYSTEM_MEMORY} MB")
    print(f"User Memory: {on_heap_user_memory} MB")
    print(f"Spark Memory: {on_heap_spark_memory} MB")
    print(f"\tStorage Memory: {on_heap_spark_storage_memory} MB")
    print(f"\tExecution Memory: {on_heap_spark_execution_memory} MB")
    if spark_memory_offheap_enabled:
        off_heap_spark_storage_memory = spark_memory_offheap_size * SPARK_MEMORY_STORAGEFRACTION
        off_heap_spark_execution_memory = spark_memory_offheap_size * (1 - SPARK_MEMORY_STORAGEFRACTION)
        print("\n")
        print(f"------------------ Off-Heap Memory: {spark_memory_offheap_size} MB ------------------")
        print(f"Storage Memory: {off_heap_spark_storage_memory} MB")
        print(f"Execution Memory: {off_heap_spark_execution_memory} MB")
        total_spark_memory += spark_memory_offheap_size
    print("\n")
    print(f"------------------ Total Spark Memory (Spark Memory + Off-Heap Memory): {total_spark_memory} MB ({total_spark_memory / GB_TO_MB_RATE} GB) ------------------")
