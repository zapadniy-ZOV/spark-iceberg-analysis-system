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

                String currentDir = System.getProperty("user.dir");
                String warehouseLocation = "file:///" + currentDir.replace("\\", "/") + "/spark-warehouse";
                SparkSession.Builder builder = SparkSession.builder()
                                .appName(appName)
                                // .master(master) // Master will be set by spark-submit
                                .config("spark.sql.warehouse.dir", warehouseLocation)
                                // S3A Hadoop configurations (for SparkHadoopCatalog's interaction with
                                // warehouse path)
                                .config("spark.hadoop.fs.s3a.endpoint", s3Endpoint)
                                .config("spark.hadoop.fs.s3a.access.key", s3AccessKeyId)
                                .config("spark.hadoop.fs.s3a.secret.key", s3SecretAccessKey)
                                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                                                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

                                // Iceberg specific configurations
                                .config("spark.sql.extensions",
                                                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                                .config(String.format("spark.sql.catalog.%s", icebergCatalog),
                                                "org.apache.iceberg.spark.SparkHadoopCatalog")
                                                .config(String.format("spark.sql.catalog.%s.type", icebergCatalog), "hadoop")
                                .config(String.format("spark.sql.catalog.%s.warehouse", icebergCatalog),
                                                icebergWarehouse)
                                .config(String.format("spark.sql.catalog.%s.io-impl", icebergCatalog),
                                                "org.apache.iceberg.hadoop.HadoopFileIO")

                                // .config(String.format("spark.sql.catalog.%s.s3.endpoint", icebergCatalog),
                                // s3Endpoint)
                                // .config(String.format("spark.sql.catalog.%s.s3.region", icebergCatalog),
                                // s3Region)
                                // .config(String.format("spark.sql.catalog.%s.s3.access-key-id",
                                // icebergCatalog),
                                // s3AccessKeyId)
                                // .config(String.format("spark.sql.catalog.%s.s3.secret-access-key",
                                // icebergCatalog),
                                // s3SecretAccessKey)
                                // .config(String.format("spark.sql.catalog.%s.s3.path-style-access",
                                // icebergCatalog),
                                // "true") // Important for S3FileIO with Yandex/MinIO

                                // --- MongoDB Connector settings ---
                                .config("spark.mongodb.read.connection.uri", mongoInputUri)
                                .config("spark.mongodb.write.connection.uri", mongoOutputUri);

                return builder.getOrCreate();
        }
}