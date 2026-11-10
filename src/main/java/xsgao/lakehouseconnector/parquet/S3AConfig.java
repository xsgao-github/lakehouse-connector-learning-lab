package xsgao.lakehouseconnector.parquet;

import org.apache.hadoop.conf.Configuration;
import xsgao.lakehouseconnector.utils.S3ClientFactory;

/**
 * Hadoop S3A settings for Parquet/FileSystem access. This configures Hadoop's
 * {@code s3a://} file system, not the AWS SDK {@code S3Client}, and is package-private:
 * only the Parquet readers/writers need it.
 */
final class S3AConfig {

    private S3AConfig() {
    }

    static Configuration create() {
        return create(S3ClientFactory.MINIO_ENDPOINT);
    }

    static Configuration create(String endpointOverride) {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.connection.ssl.enabled", "false");
        conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        conf.set("fs.s3a.access.key", S3ClientFactory.MINIO_ACCESS_KEY);
        conf.set("fs.s3a.secret.key", S3ClientFactory.MINIO_SECRET_KEY);

        // Upload through direct memory instead of the default local disk buffer: the disk
        // path calls Hadoop native access checks, which fail on Windows without winutils
        // (UnsatisfiedLinkError in NativeIO.Windows.access0).
        conf.setBoolean("fs.s3a.fast.upload", false);
        conf.set("fs.s3a.fast.upload.buffer", "bytebuffer");

        if (endpointOverride != null) {
            conf.set("fs.s3a.endpoint", endpointOverride);
        }
        return conf;
    }
}
