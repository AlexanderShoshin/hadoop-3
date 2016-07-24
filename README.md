## Testing environment

Programm was tested on HDP 2.4 sandbox.

## How to deploy

1. Make hadoop-3-1.0-SNAPSHOT.jar by running maven command:
```
mvn clean package
```
2. Copy hadoop-3-1.0-SNAPSHOT.jar to machine with hadoop installed.
3. Download dataset(https://drive.google.com/file/d/0B4eU5TenoBPjZllmdTVfRS1xSE0/view) and put it to hdfs.
4. Run MapReduce job, passing hdfs path to dataset and hdfs path to output directory:
```
hadoop jar <path_to_jar>/hadoop-3-1.0-SNAPSHOT.jar <hdfs_path_to_input_file> <hdfs_path_to_output_directory>
```