package xsgao.lakehouseconnector.compaction;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Policy knobs for small-file compaction. The values are intentionally conservative:
 * production lakehouses typically target 128-512 MiB files and treat anything below
 * 8 MiB as a small file.
 */
public final class CompactionOptions {

    public static final long DEFAULT_SMALL_FILE_THRESHOLD_BYTES = 8L * 1024 * 1024;
    public static final int DEFAULT_MIN_INPUT_FILES_PER_GROUP = 3;
    public static final long DEFAULT_TARGET_FILE_SIZE_BYTES = 128L * 1024 * 1024;
    public static final int DEFAULT_ROW_GROUP_SIZE_BYTES = 8 * 1024 * 1024;

    private final long smallFileThresholdBytes;
    private final int minInputFilesPerGroup;
    private final long targetFileSizeBytes;
    private final int rowGroupSizeBytes;
    private final CompressionCodecName codec;

    public CompactionOptions(long smallFileThresholdBytes, int minInputFilesPerGroup, long targetFileSizeBytes) {
        this(smallFileThresholdBytes, minInputFilesPerGroup, targetFileSizeBytes,
                DEFAULT_ROW_GROUP_SIZE_BYTES, CompressionCodecName.SNAPPY);
    }

    public CompactionOptions(long smallFileThresholdBytes, int minInputFilesPerGroup, long targetFileSizeBytes,
                             int rowGroupSizeBytes, CompressionCodecName codec) {
        if (smallFileThresholdBytes <= 0) {
            throw new IllegalArgumentException("smallFileThresholdBytes must be positive");
        }
        if (minInputFilesPerGroup < 2) {
            throw new IllegalArgumentException("minInputFilesPerGroup must be at least 2");
        }
        if (targetFileSizeBytes <= 0) {
            throw new IllegalArgumentException("targetFileSizeBytes must be positive");
        }
        if (rowGroupSizeBytes <= 0) {
            throw new IllegalArgumentException("rowGroupSizeBytes must be positive");
        }
        if (codec == null) {
            throw new IllegalArgumentException("codec must not be null");
        }
        this.smallFileThresholdBytes = smallFileThresholdBytes;
        this.minInputFilesPerGroup = minInputFilesPerGroup;
        this.targetFileSizeBytes = targetFileSizeBytes;
        this.rowGroupSizeBytes = rowGroupSizeBytes;
        this.codec = codec;
    }

    public static CompactionOptions defaults() {
        return new CompactionOptions(DEFAULT_SMALL_FILE_THRESHOLD_BYTES,
                DEFAULT_MIN_INPUT_FILES_PER_GROUP, DEFAULT_TARGET_FILE_SIZE_BYTES);
    }

    public long getSmallFileThresholdBytes() {
        return smallFileThresholdBytes;
    }

    public int getMinInputFilesPerGroup() {
        return minInputFilesPerGroup;
    }

    public long getTargetFileSizeBytes() {
        return targetFileSizeBytes;
    }

    public int getRowGroupSizeBytes() {
        return rowGroupSizeBytes;
    }

    public CompressionCodecName getCodec() {
        return codec;
    }
}
