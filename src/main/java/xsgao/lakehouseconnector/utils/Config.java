package xsgao.lakehouseconnector.utils;

public class Config {
    // Read from JVM system properties (-Dlakehouse.s3.max.retries=5) or default to 3
    public static final int S3_MAX_RETRIES = Integer.parseInt(
        System.getProperty("s3.max.retries", "3")
    );
    
    public static final long S3_BASE_DELAY_MS = Long.parseLong(
        System.getProperty("s3.base.delay.ms", "100")
    );
    
    public static final long S3_MAX_DELAY_MS = Long.parseLong(
        System.getProperty("s3.max.delay.ms", "1000")
    );
}
