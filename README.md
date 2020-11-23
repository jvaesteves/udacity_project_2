### Step 3
#### Write the answers to these questions in the README.md doc of your GitHub repo:

**How did changing values on the SparkSession property parameters affect the throughput and latency of the data?** 

Parameters like **maxOffsetsPerTrigger**, that limits the maximum number of offsets processed per trigger interval, specified total number of offsets will be proportionally split across topicPartitions of different volume. And **maxRatePerPartition**, which is the maximum rate (in messages per second) at which each Kafka partition will be read by this direct API.

**What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?**

I achieved the best performance with the following configuration: 
* master(local(*)). This will use all available cores in CPU.
* config("spark.sql.shuffle.partitions", 4). This parameter impromeved my **processedRowsPerSecond**.
* option("maxOffsetsPerTrigger", 8000). This parameter impromeved my **processedRowsPerSecond**.
