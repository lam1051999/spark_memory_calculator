# Spark memory calculator

Scripts to calculate total memory used in Spark executors.

```bash
cd calculate
mvn clean package
cd ..
python calculate.py

# output
Amount of spark.executor.memory (in GB): 1
Option spark.memory.offHeap.enabled: true
Amount of spark.memory.offHeap.size (in GB): 0.5 


------------------ On-Heap Memory: 910.5 MB ------------------
Researved Memory: 300 MB
User Memory: 244.20000000000002 MB
Spark Memory: 366.3 MB
        Storage Memory: 183.15 MB
        Execution Memory: 183.15 MB


------------------ Off-Heap Memory: 512.0 MB ------------------
Storage Memory: 256.0 MB
Execution Memory: 256.0 MB


------------------ Total Spark Memory (Spark Memory + Off-Heap Memory): 878.3 MB (0.85771484375 GB) ------------------
```