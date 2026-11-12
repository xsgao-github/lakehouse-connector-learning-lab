package xsgao.lakehouseconnector.parquet;

/**
 * Per-column encoding choice for {@link VectorizedParquetTableWriter}.
 *
 * <p>{@code DELTA} maps to {@code DELTA_BINARY_PACKED} for INT32/INT64 and to
 * {@code DELTA_LENGTH_BYTE_ARRAY} for BINARY; other types fall back to dictionary/PLAIN.
 * {@code DICTIONARY} maps to a dictionary page plus RLE/bit-packed indices.
 */
public enum ColumnEncoding {
    PLAIN,
    DICTIONARY,
    DELTA
}
