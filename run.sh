spark-submit --master local[*] --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.472 main.py
$env:PYSPARK_PYTHON = "C:\Program Files\Python310\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "C:\Program Files\Python310\python.exe"