package com.temesoft.fs;

import javax.crypto.SecretKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;

/**
 * A transparent, chunked-encryption decorator for any {@link FileStorageService}.
 * This wrapper provides data-at-rest protection by encrypting content before it is written
 * to the underlying storage and decrypting it upon retrieval. Files are processed in
 * discrete chunks to allow for efficient random-access reads (range requests) without
 * needing to decrypt the entire file.
 * <p>
 * <b>Storage Layout:</b>
 * <pre>
 * [Header]                       - Metadata (version, chunkSize, etc.)
 * [Encrypted Chunk 0]            - [12-byte nonce][ciphertext + 16-byte GCM tag]
 * [Encrypted Chunk 1]            - [12-byte nonce][ciphertext + 16-byte GCM tag]
 * ...
 * </pre>
 *
 * @param <T> The type of file identifier used by the service.
 */
public class EncryptingFileStorageServiceWrapper<T> implements FileStorageServiceWrapper<T> {

    private final FileStorageService<T> service;
    private final FileStorageCryptor cryptor;
    private final int chunkSize;

    public EncryptingFileStorageServiceWrapper(
            final FileStorageService<T> service,
            final FileStorageCryptor cryptor,
            final int chunkSize
    ) {
        this.service = Objects.requireNonNull(service, "service must not be null");
        this.cryptor = Objects.requireNonNull(cryptor, "cryptor must not be null");
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0");
        }
        this.chunkSize = chunkSize;
    }

    /**
     * @return The underlying (decorated) file storage service.
     */
    @Override
    public FileStorageService<T> getService() {
        return service;
    }

    /**
     * Checks if the file exists in the underlying storage.
     */
    @Override
    public boolean exists(final T id) throws FileStorageException {
        return service.exists(id);
    }

    /**
     * Retrieves the original plaintext size of the file.
     * Note: This is read from the encrypted header and will be smaller than the
     * actual size stored on disk due to encryption overhead (header and tags).
     *
     * @param id The file identifier.
     * @return The size of the file in bytes before encryption.
     */
    @Override
    public long getSize(final T id) throws FileStorageException {
        final EncryptedFileHeader header = readHeader(id);
        return header.plaintextSize();
    }

    /**
     * Encrypts and stores the provided byte array.
     */
    @Override
    public void create(final T id, final byte[] bytes) throws FileStorageException {
        Objects.requireNonNull(bytes, "bytes must not be null");
        final byte[] encrypted = encrypt(bytes);
        service.create(id, encrypted);
    }

    /**
     * Encrypts and stores the provided input stream of specified length
     */
    @Override
    public void create(final T id, final InputStream inputStream, final long contentSize) throws FileStorageException {
        Objects.requireNonNull(inputStream, "inputStream must not be null");
        if (contentSize < 0) {
            throw new IllegalArgumentException("contentSize must be >= 0");
        }

        try {
            final byte[] plaintext = readAllBytesExactly(inputStream, contentSize);
            create(id, plaintext);
        } catch (final IOException e) {
            throw new FileStorageException("Unable to read input stream for encrypted create", e);
        }
    }

    /**
     * Forwards the request to delete file to underlying storage service
     */
    @Override
    public void delete(final T id) throws FileStorageException {
        service.delete(id);
    }

    /**
     * Retrieves and decrypts the entire file content.
     */
    @Override
    public byte[] getBytes(final T id) throws FileStorageException {
        final byte[] encrypted = service.getBytes(id);
        return decrypt(encrypted);
    }

    /**
     * Retrieves and decrypts a specific range of the file.
     * This implementation is optimized to only fetch and decrypt the specific
     * chunks that overlap with the requested plaintext range.
     *
     * @param id            The file identifier.
     * @param startPosition The inclusive start index in the plaintext.
     * @param endPosition   The exclusive end index in the plaintext.
     * @return The decrypted byte range.
     * @throws IllegalArgumentException if the range is invalid or exceeds file bounds.
     */
    @Override
    public byte[] getBytes(final T id, final long startPosition, final long endPosition) throws FileStorageException {
        if (startPosition < 0) {
            throw new IllegalArgumentException("startPosition must be >= 0");
        }
        if (endPosition < startPosition) {
            throw new IllegalArgumentException("endPosition must be >= startPosition");
        }
        if (startPosition == endPosition) {
            return new byte[0];
        }

        final EncryptedFileHeader header = readHeader(id);
        final long plaintextSize = header.plaintextSize();

        if (endPosition > plaintextSize) {
            throw new IllegalArgumentException(
                    "Requested range [" + startPosition + ", " + endPosition + ") exceeds plaintext size " + plaintextSize
            );
        }

        final SecretKey key = cryptor.resolveKey(header);
        final int startChunk = Math.toIntExact(startPosition / header.chunkSize());
        final int endChunk = Math.toIntExact((endPosition - 1) / header.chunkSize());

        final ByteArrayOutputStream output = new ByteArrayOutputStream(Math.toIntExact(endPosition - startPosition));

        for (int chunkIndex = startChunk; chunkIndex <= endChunk; chunkIndex++) {
            final byte[] encryptedChunk = readEncryptedChunk(id, header, chunkIndex);
            final byte[] plaintextChunk = cryptor.decryptChunk(key, header, chunkIndex, encryptedChunk);

            final long chunkPlainStart = (long) chunkIndex * header.chunkSize();
            final long chunkPlainEnd = Math.min(chunkPlainStart + plaintextChunk.length, plaintextSize);

            final int from = (int) Math.max(0, startPosition - chunkPlainStart);
            final int to = (int) Math.min(plaintextChunk.length, endPosition - chunkPlainStart);

            if (from < 0 || to < 0 || from > plaintextChunk.length || to > plaintextChunk.length || from > to) {
                throw new FileStorageException("Invalid slice boundaries while reading encrypted range");
            }

            output.write(plaintextChunk, from, to - from);

            if (chunkPlainEnd >= endPosition) {
                break;
            }
        }

        return output.toByteArray();
    }

    @Override
    public InputStream getInputStream(final T id) throws FileStorageException {
        return new ByteArrayInputStream(getBytes(id));
    }

    @Override
    public void deleteAll() throws FileStorageException {
        service.deleteAll();
    }

    @Override
    public String getStorageDescription() {
        return "Encrypted(" + service.getStorageDescription() + ")";
    }

    @Override
    public FileStorageIdService getFileStorageIdService() {
        return service.getFileStorageIdService();
    }

    /**
     * Internal helper to handle the chunked encryption process.
     *
     * @param plaintext The raw bytes to encrypt.
     * @return The full binary payload (header + encrypted chunks).
     */
    private byte[] encrypt(final byte[] plaintext) {
        final EncryptedFileHeader header = cryptor.newHeader(plaintext.length, chunkSize);
        final SecretKey key = cryptor.resolveKey(header);
        final byte[] headerBytes = header.serialize();

        final ByteArrayOutputStream output = new ByteArrayOutputStream(
                headerBytes.length + header.chunkCount() * cryptor.encryptedChunkSize(header.chunkSize())
        );
        output.writeBytes(headerBytes);

        for (int chunkIndex = 0; chunkIndex < header.chunkCount(); chunkIndex++) {
            final int offset = chunkIndex * header.chunkSize();
            final int length = Math.min(header.chunkSize(), plaintext.length - offset);
            final byte[] encryptedChunk = cryptor.encryptChunk(key, header, chunkIndex, plaintext, offset, length);
            output.writeBytes(encryptedChunk);
        }

        return output.toByteArray();
    }

    /**
     * Internal helper to handle the chunked decryption process for a full payload.
     *
     * @param encrypted The full binary payload read from storage.
     * @return The original plaintext bytes.
     */
    private byte[] decrypt(final byte[] encrypted) {
        if (encrypted.length < EncryptedFileHeader.FIXED_PART_SIZE) {
            throw new FileStorageException("Encrypted payload is too short to contain a header");
        }

        final EncryptedFileHeader header = parseHeader(encrypted);
        final SecretKey key = cryptor.resolveKey(header);

        final ByteArrayOutputStream output = new ByteArrayOutputStream(Math.toIntExact(header.plaintextSize()));
        int offset = header.headerSize();

        for (int chunkIndex = 0; chunkIndex < header.chunkCount(); chunkIndex++) {
            final int remainingPlaintext = (int) Math.min(
                    header.chunkSize(),
                    header.plaintextSize() - ((long) chunkIndex * header.chunkSize())
            );
            final int encryptedChunkSize = cryptor.encryptedChunkSize(remainingPlaintext);

            if (offset + encryptedChunkSize > encrypted.length) {
                throw new FileStorageException("Encrypted payload ended prematurely at chunk " + chunkIndex);
            }

            final byte[] encryptedChunk = Arrays.copyOfRange(encrypted, offset, offset + encryptedChunkSize);
            final byte[] plaintextChunk = cryptor.decryptChunk(key, header, chunkIndex, encryptedChunk);
            output.writeBytes(plaintextChunk);
            offset += encryptedChunkSize;
        }

        return output.toByteArray();
    }

    /**
     * Reads and parses the {@link EncryptedFileHeader} from the underlying storage.
     *
     * @param id The file identifier.
     * @return The parsed header.
     */
    private EncryptedFileHeader readHeader(final T id) {
        final byte[] fixedHeader = service.getBytes(id, 0, EncryptedFileHeader.FIXED_PART_SIZE);
        if (fixedHeader.length < EncryptedFileHeader.FIXED_PART_SIZE) {
            throw new FileStorageException("Stored encrypted file is too short to contain a valid header");
        }

        final int keyIdLength = ((fixedHeader[30] & 0xFF) << 8) | (fixedHeader[31] & 0xFF);
        final int totalHeaderSize = EncryptedFileHeader.FIXED_PART_SIZE + keyIdLength;

        final byte[] fullHeader = service.getBytes(id, 0, totalHeaderSize);
        if (fullHeader.length < totalHeaderSize) {
            throw new FileStorageException("Stored encrypted file header is incomplete");
        }

        return EncryptedFileHeader.parse(fullHeader);
    }

    /**
     * Parses the binary header from an encrypted payload already loaded into memory.
     *
     * @param encrypted The full encrypted byte array.
     * @return The parsed {@link EncryptedFileHeader}.
     * @throws FileStorageException if the byte array is too short or the header is malformed.
     */
    private EncryptedFileHeader parseHeader(final byte[] encrypted) {
        final byte[] fixedHeader = Arrays.copyOfRange(encrypted, 0, EncryptedFileHeader.FIXED_PART_SIZE);
        final int keyIdLength = ((fixedHeader[30] & 0xFF) << 8) | (fixedHeader[31] & 0xFF);
        final int totalHeaderSize = EncryptedFileHeader.FIXED_PART_SIZE + keyIdLength;

        if (encrypted.length < totalHeaderSize) {
            throw new FileStorageException("Encrypted payload does not contain the full header");
        }

        final byte[] fullHeader = Arrays.copyOfRange(encrypted, 0, totalHeaderSize);
        return EncryptedFileHeader.parse(fullHeader);
    }

    /**
     * Reads a specific encrypted chunk from the underlying storage.
     * This method calculates the exact physical byte offset of the requested chunk by
     * accounting for the variable-length header and the cumulative size of preceding
     * encrypted chunks (plaintext + nonce + tag).
     *
     * @param id         The file identifier.
     * @param header     The file header used to calculate offsets.
     * @param chunkIndex The zero-based index of the chunk to retrieve.
     * @return The raw encrypted chunk bytes (including nonce and tag).
     * @throws FileStorageException if the physical read from the underlying service fails.
     */
    private byte[] readEncryptedChunk(final T id, final EncryptedFileHeader header, final int chunkIndex) {
        final long plainChunkStart = (long) chunkIndex * header.chunkSize();
        final int plainChunkSize = (int) Math.min(header.chunkSize(), header.plaintextSize() - plainChunkStart);
        final int encryptedChunkSize = cryptor.encryptedChunkSize(plainChunkSize);

        long encryptedStart = header.headerSize();
        for (int i = 0; i < chunkIndex; i++) {
            final long previousPlainStart = (long) i * header.chunkSize();
            final int previousPlainChunkSize = (int) Math.min(header.chunkSize(), header.plaintextSize() - previousPlainStart);
            encryptedStart += cryptor.encryptedChunkSize(previousPlainChunkSize);
        }
        final long encryptedEnd = encryptedStart + encryptedChunkSize;

        return service.getBytes(id, encryptedStart, encryptedEnd);
    }

    /**
     * Reads a specific number of bytes from an {@link InputStream}, ensuring the exact
     * amount is retrieved or an error is thrown.
     * This is used during the encryption process to ensure the plaintext is fully
     * captured before chunking begins.
     *
     * @param inputStream The source stream.
     * @param contentSize The exact number of bytes to read.
     * @return A byte array of the requested size.
     * @throws IOException if the stream ends prematurely or a read error occurs.
     */
    private static byte[] readAllBytesExactly(final InputStream inputStream, final long contentSize) throws IOException {
        final ByteArrayOutputStream output = new ByteArrayOutputStream(Math.toIntExact(contentSize));
        final byte[] buffer = new byte[8192];
        long remaining = contentSize;

        while (remaining > 0) {
            final int read = inputStream.read(buffer, 0, (int) Math.min(buffer.length, remaining));
            if (read == -1) {
                throw new IOException("InputStream ended before contentSize bytes were read");
            }
            output.write(buffer, 0, read);
            remaining -= read;
        }
        return output.toByteArray();
    }
}