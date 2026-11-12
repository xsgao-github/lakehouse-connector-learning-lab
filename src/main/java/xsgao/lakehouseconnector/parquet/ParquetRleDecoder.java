package xsgao.lakehouseconnector.parquet;

import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;

import java.util.Arrays;

/**
 * Bulk decoder for Parquet's RLE/bit-packed hybrid stream (dictionary ids and definition
 * levels).
 *
 * <p>RLE runs become one {@link Arrays#fill}; bit-packed runs are unpacked 32 values at a
 * time with parquet-mr's {@link Packer}, keeping the inner loop monomorphic and
 * JIT-friendly instead of using a per-value {@code readInt()} state machine.
 */
final class ParquetRleDecoder {

    private ParquetRleDecoder() {
    }

    /**
     * Decodes {@code count} values of {@code bitWidth} bits from {@code in} starting at
     * {@code offset} (the offset points after the 1-byte bit width prefix).
     */
    static int[] decode(int bitWidth, byte[] in, int offset, int count) {
        int[] out = new int[count];
        if (count == 0 || bitWidth == 0) {
            return out;
        }

        BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
        int[] tailGroup = new int[8];
        int position = offset;
        int outPosition = 0;

        while (outPosition < count) {
            int header = readUnsignedVarInt(in, position);
            position += varIntSize(header);

            if ((header & 1) == 0) {
                int runLength = header >>> 1;
                int value = readIntLittleEndianPadded(in, position, bitWidth);
                position += (bitWidth + 7) >>> 3;
                int end = Math.min(count, outPosition + runLength);
                Arrays.fill(out, outPosition, end, value);
                outPosition = end;
            } else {
                int groups = header >>> 1;
                int packedBytes = groups * bitWidth;
                int available = Math.min(groups * 8, count - outPosition);

                int packedPosition = position;
                int writePosition = outPosition;
                int chunks32 = available >>> 5;
                for (int i = 0; i < chunks32; i++) {
                    packer.unpack32Values(in, packedPosition, out, writePosition);
                    packedPosition += 4 * bitWidth;
                    writePosition += 32;
                }

                int groups8 = (available - chunks32 * 32) >>> 3;
                for (int i = 0; i < groups8; i++) {
                    packer.unpack8Values(in, packedPosition, out, writePosition);
                    packedPosition += bitWidth;
                    writePosition += 8;
                }

                int tail = available - chunks32 * 32 - groups8 * 8;
                if (tail > 0) {
                    packer.unpack8Values(in, packedPosition, tailGroup, 0);
                    System.arraycopy(tailGroup, 0, out, writePosition, tail);
                }

                position += packedBytes;
                outPosition += available;
            }
        }
        return out;
    }

    private static int readUnsignedVarInt(byte[] in, int position) {
        int value = 0;
        int shift = 0;
        while (true) {
            int b = in[position++] & 0xFF;
            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
            shift += 7;
        }
    }

    private static int varIntSize(int value) {
        int size = 1;
        while ((value >>>= 7) != 0) {
            size++;
        }
        return size;
    }

    private static int readIntLittleEndianPadded(byte[] in, int position, int bitWidth) {
        int bytes = (bitWidth + 7) >>> 3;
        int value = 0;
        for (int i = 0; i < bytes; i++) {
            value |= (in[position + i] & 0xFF) << (8 * i);
        }
        return value;
    }
}
