package itmo.rshd.hadoop.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Value("${spark.appName}")
    private String appName;

    @Value("${spark.master}")
    private String master;

    @Value("${spark.iceberg.catalog}")
    private String icebergCatalog;

    @Value("${spark.iceberg.warehouse}")
    private String icebergWarehouse;

    @Value("${aws.s3.endpoint}")
    private String s3Endpoint;

    @Value("${aws.s3.accessKeyId}")
    private String s3AccessKeyId;

    @Value("${aws.s3.secretAccessKey}")
    private String s3SecretAccessKey;
    
    @Value("${aws.s3.region}")
    private String s3Region;

    @Value("${mongodb.uri}")
    private String mongoDbUri;

    @Value("${mongodb.database}")
    private String mongoDbDatabase;


    @Bean
    public SparkSession sparkSession() {
        String mongoInputUri = String.format("%s/%s", mongoDbUri, mongoDbDatabase);
        String mongoOutputUri = String.format("%s/%s", mongoDbUri, mongoDbDatabase);

        // Define a warehouse path for Spark SQL and Hive Metastore (if used locally)
        String currentDir = System.getProperty("user.dir");
        // Ensure paths are constructed correctly for Windows/Linux (replace backslashes for URI compatibility if needed)
        String warehouseLocation = "file:///" + currentDir.replace("\\", "/") + "/spark-warehouse";
        // String derbyDbLocation = currentDir.replace("\\", "/") + "/metastore_db"; // No longer needed for Hadoop catalog

        return SparkSession.builder()
                .appName(appName)
                .master(master)
                .config("spark.sql.warehouse.dir", warehouseLocation) // General Spark warehouse, not for Iceberg with Hadoop catalog
                // Hive Metastore and DataNucleus properties are removed as we are using SparkHadoopCatalog for Iceberg
                // S3A Hadoop configurations
                .config("hadoop.fs.s3a.endpoint", s3Endpoint)
                .config("hadoop.fs.s3a.access.key", s3AccessKeyId)
                .config("hadoop.fs.s3a.secret.key", s3SecretAccessKey)
                .config("hadoop.fs.s3a.path.style.access", "true") // Important for MinIO or other S3 compatible storage
                .config("hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                // Iceberg specific configurations
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config(String.format("spark.sql.catalog.%s", icebergCatalog), "org.apache.iceberg.spark.SparkHadoopCatalog") // Changed to SparkHadoopCatalog
                .config(String.format("spark.sql.catalog.%s.warehouse", icebergCatalog), icebergWarehouse)
                .config(String.format("spark.sql.catalog.%s.io-impl", icebergCatalog), "org.apache.iceberg.aws.s3.S3FileIO")
                // Configure S3FileIO for Iceberg to use the same credentials and region
                .config(String.format("spark.sql.catalog.%s.s3.endpoint", icebergCatalog), s3Endpoint)
                .config(String.format("spark.sql.catalog.%s.s3.region", icebergCatalog), s3Region)
                .config(String.format("spark.sql.catalog.%s.s3.access-key-id", icebergCatalog), s3AccessKeyId)
                .config(String.format("spark.sql.catalog.%s.s3.secret-access-key", icebergCatalog), s3SecretAccessKey)
                // It's generally better to rely on the DefaultAWSCredentialsProviderChain which can pick up
                // credentials from environment variables, IAM roles, or AWS credentials file.
                // However, if explicit configuration is needed and the above Hadoop props don't suffice for Iceberg's S3FileIO:
                 .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

                // MongoDB Connector settings
                .config("spark.mongodb.read.connection.uri", mongoInputUri)
                .config("spark.mongodb.write.connection.uri", mongoOutputUri)
                // .enableHiveSupport() // Removed: Not needed for SparkHadoopCatalog and was causing issues
                .getOrCreate();
    }
} 