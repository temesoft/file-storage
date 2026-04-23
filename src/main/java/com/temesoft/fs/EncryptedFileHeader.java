package com.temesoft.fs;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Represents the metadata header stored at the beginning of every encrypted file.
 * The header contains information necessary to decrypt the file, including the algorithm version,
 * chunking parameters, and the unique nonce prefix used for cryptographic operations.
 * <p>
 * <b>Binary Layout:</b>
 * <pre>
 * magic                4 bytes  = 'T','F','S','E'
 * version              1 byte   = 1
 * algorithm            1 byte   = 1 (AES/GCM chunked)
 * chunkSize            4 bytes
 * plaintextSize        8 bytes
 * chunkCount           4 bytes
 * fileNoncePrefix      8 bytes
 * keyIdLength          2 bytes  (unsigned short)
 * keyId                N bytes  (UTF-8 encoded)
 * </pre>
 * Total header size = 32 + keyIdLength.
 */
public final class EncryptedFileHeader {

    static final byte[] MAGIC = new byte[]{'T', 'F', 'S', 'E'};
    static final byte VERSION_1 = 1;
    static final byte ALG_AES_GCM_CHUNKED = 1;
    static final int FIXED_PART_SIZE = 4 + 1 + 1 + 4 + 8 + 4 + 8 + 2;

    private final byte version;
    private final byte algorithm;
    private final int chunkSize;
    private final long plaintextSize;
    private final int chunkCount;
    private final byte[] fileNoncePrefix; // 8 bytes
    private final String keyId;

    /**
     * Constructs a new header with validated parameters.
     *
     * @param version         The header format version
     * @param algorithm       The encryption algorithm identifier
     * @param chunkSize       The size of data chunks in bytes
     * @param plaintextSize   The original size of the file before encryption
     * @param chunkCount      The total number of chunks in the file
     * @param fileNoncePrefix The 8-byte unique random prefix for this file
     * @param keyId           The identifier of the key used for encryption
     * @throws IllegalArgumentException if any parameter is invalid or out of range
     */
    public EncryptedFileHeader(
            final byte version,
            final byte algorithm,
            final int chunkSize,
            final long plaintextSize,
            final int chunkCount,
            final byte[] fileNoncePrefix,
            final String keyId
    ) {
        if (version != VERSION_1) {
            throw new IllegalArgumentException("Unsupported header version: " + version);
        }
        if (algorithm != ALG_AES_GCM_CHUNKED) {
            throw new IllegalArgumentException("Unsupported algorithm: " + algorithm);
        }
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0");
        }
        if (plaintextSize < 0) {
            throw new IllegalArgumentException("plaintextSize must be >= 0");
        }
        if (chunkCount < 0) {
            throw new IllegalArgumentException("chunkCount must be >= 0");
        }
        if (fileNoncePrefix == null || fileNoncePrefix.length != 8) {
            throw new IllegalArgumentException("fileNoncePrefix must be 8 bytes");
        }
        this.version = version;
        this.algorithm = algorithm;
        this.chunkSize = chunkSize;
        this.plaintextSize = plaintextSize;
        this.chunkCount = chunkCount;
        this.fileNoncePrefix = Arrays.copyOf(fileNoncePrefix, fileNoncePrefix.length);
        this.keyId = Objects.requireNonNullElse(keyId, "");
    }

    public byte version() {
        return version;
    }

    public byte algorithm() {
        return algorithm;
    }

    public int chunkSize() {
        return chunkSize;
    }

    public long plaintextSize() {
        return plaintextSize;
    }

    public int chunkCount() {
        return chunkCount;
    }

    public byte[] fileNoncePrefix() {
        return Arrays.copyOf(fileNoncePrefix, fileNoncePrefix.length);
    }

    public String keyId() {
        return keyId;
    }

    public int headerSize() {
        return FIXED_PART_SIZE + keyId.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }

    /**
     * Serializes the header into a byte array for storage at the start of a file.
     *
     * @return A byte array representing the binary header
     * @throws IllegalArgumentException if the keyId exceeds the 2-byte length limit (65535 bytes)
     */
    public byte[] serialize() {
        final byte[] keyIdBytes = keyId.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (keyIdBytes.length > 0xFFFF) {
            throw new IllegalArgumentException("keyId is too long");
        }

        final ByteBuffer buffer = ByteBuffer.allocate(FIXED_PART_SIZE + keyIdBytes.length);
        buffer.put(MAGIC);
        buffer.put(version);
        buffer.put(algorithm);
        buffer.putInt(chunkSize);
        buffer.putLong(plaintextSize);
        buffer.putInt(chunkCount);
        buffer.put(fileNoncePrefix);
        buffer.putShort((short) keyIdBytes.length);
        buffer.put(keyIdBytes);
        return buffer.array();
    }

    /**
     * Parses a byte array into an {@code EncryptedFileHeader} instance.
     *
     * @param headerBytes The raw bytes read from the beginning of a file
     * @return A populated header instance
     * @throws IllegalArgumentException if the magic bytes are wrong or the data is incomplete/invalid
     */
    public static EncryptedFileHeader parse(final byte[] headerBytes) {
        if (headerBytes == null || headerBytes.length < FIXED_PART_SIZE) {
            throw new IllegalArgumentException("Not enough bytes to parse encrypted file header");
        }
        final ByteBuffer buffer = ByteBuffer.wrap(headerBytes);

        final byte[] magic = new byte[4];
        buffer.get(magic);
        if (!Arrays.equals(MAGIC, magic)) {
            throw new IllegalArgumentException("Invalid encrypted file header magic");
        }

        final byte version = buffer.get();
        final byte algorithm = buffer.get();
        final int chunkSize = buffer.getInt();
        final long plaintextSize = buffer.getLong();
        final int chunkCount = buffer.getInt();

        final byte[] fileNoncePrefix = new byte[8];
        buffer.get(fileNoncePrefix);

        final int keyIdLength = Short.toUnsignedInt(buffer.getShort());
        if (headerBytes.length < FIXED_PART_SIZE + keyIdLength) {
            throw new IllegalArgumentException("Incomplete encrypted file header");
        }

        final byte[] keyIdBytes = new byte[keyIdLength];
        buffer.get(keyIdBytes);

        return new EncryptedFileHeader(
                version,
                algorithm,
                chunkSize,
                plaintextSize,
                chunkCount,
                fileNoncePrefix,
                new String(keyIdBytes, java.nio.charset.StandardCharsets.UTF_8)
        );
    }
}
