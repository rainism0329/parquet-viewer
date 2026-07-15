package com.bigphil.parquetviewer;

import com.github.luben.zstd.Zstd;
import io.airlift.compress.lz4.Lz4Decompressor;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;

/**
 * Read-only Parquet codecs that do not initialize Hadoop's runtime.
 *
 * <p>Parquet's default local-file path still creates a Hadoop Configuration. The
 * viewer selects the compact codec implementations already present in Parquet's
 * dependency graph instead of shipping a distributed-computing client stack.</p>
 */
final class NativeCompressionCodecFactory implements CompressionCodecFactory {
    @Override
    public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
        throw new UnsupportedOperationException("The viewer does not write Parquet pages");
    }

    @Override
    public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
        return switch (codecName) {
            case UNCOMPRESSED -> new ArrayDecompressor(codecName) {
                @Override
                protected int decompress(byte[] input, byte[] output) throws IOException {
                    if (input.length != output.length) {
                        throw invalidSize(codecName, output.length, input.length);
                    }
                    System.arraycopy(input, 0, output, 0, input.length);
                    return input.length;
                }
            };
            case SNAPPY -> new ArrayDecompressor(codecName) {
                @Override
                protected int decompress(byte[] input, byte[] output) throws IOException {
                    return Snappy.uncompress(input, 0, input.length, output, 0);
                }
            };
            case GZIP -> new ArrayDecompressor(codecName) {
                @Override
                protected int decompress(byte[] input, byte[] output) throws IOException {
                    try (InputStream stream = new GZIPInputStream(new ByteArrayInputStream(input))) {
                        int position = 0;
                        while (position < output.length) {
                            int read = stream.read(output, position, output.length - position);
                            if (read < 0) break;
                            position += read;
                        }
                        if (position == output.length && stream.read() != -1) {
                            throw invalidSize(codecName, output.length, output.length + 1);
                        }
                        return position;
                    }
                }
            };
            case ZSTD -> new ArrayDecompressor(codecName) {
                @Override
                protected int decompress(byte[] input, byte[] output) throws IOException {
                    long result = Zstd.decompressByteArray(
                            output, 0, output.length, input, 0, input.length);
                    if (Zstd.isError(result)) {
                        throw new IOException("Unable to decompress ZSTD Parquet page: "
                                + Zstd.getErrorName(result));
                    }
                    if (result > Integer.MAX_VALUE) {
                        throw new IOException("Decompressed ZSTD page is too large");
                    }
                    return (int) result;
                }
            };
            case LZ4_RAW -> new ArrayDecompressor(codecName) {
                @Override
                protected int decompress(byte[] input, byte[] output) {
                    return new Lz4Decompressor().decompress(
                            input, 0, input.length, output, 0, output.length);
                }
            };
            case LZO, BROTLI, LZ4 -> throw new IllegalArgumentException(
                    "Compression codec " + codecName + " is not supported by the lightweight viewer runtime. "
                            + "Use UNCOMPRESSED, SNAPPY, GZIP, ZSTD, or LZ4_RAW.");
        };
    }

    @Override
    public void release() {
        // Stateless; individual GZIP streams are closed after each page.
    }

    private abstract static class ArrayDecompressor implements BytesInputDecompressor {
        private final CompressionCodecName codecName;

        private ArrayDecompressor(CompressionCodecName codecName) {
            this.codecName = codecName;
        }

        @Override
        public final BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
            if (uncompressedSize < 0) {
                throw new IOException("Invalid negative Parquet page size: " + uncompressedSize);
            }
            byte[] output = new byte[uncompressedSize];
            int actualSize = decompress(bytes.toByteArray(), output);
            validateSize(codecName, uncompressedSize, actualSize);
            return BytesInput.from(output);
        }

        @Override
        public final void decompress(ByteBuffer input, int compressedSize,
                                     ByteBuffer output, int uncompressedSize) throws IOException {
            if (compressedSize < 0 || compressedSize > input.remaining()) {
                throw new IOException("Invalid compressed Parquet page size: " + compressedSize);
            }
            if (uncompressedSize < 0 || uncompressedSize > output.remaining()) {
                throw new IOException("Invalid uncompressed Parquet page size: " + uncompressedSize);
            }

            byte[] compressed = new byte[compressedSize];
            input.get(compressed);
            byte[] uncompressed = new byte[uncompressedSize];
            int actualSize = decompress(compressed, uncompressed);
            validateSize(codecName, uncompressedSize, actualSize);
            output.put(uncompressed);
        }

        protected abstract int decompress(byte[] input, byte[] output) throws IOException;

        @Override
        public void release() {
            // Stateless.
        }
    }

    private static void validateSize(CompressionCodecName codecName, int expected, int actual)
            throws IOException {
        if (actual != expected) {
            throw invalidSize(codecName, expected, actual);
        }
    }

    private static IOException invalidSize(CompressionCodecName codecName, int expected, int actual) {
        return new IOException("Invalid " + codecName + " Parquet page size: expected "
                + expected + " bytes, decoded " + actual);
    }
}
