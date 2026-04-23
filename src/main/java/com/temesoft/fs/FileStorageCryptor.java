package com.temesoft.fs;

import javax.crypto.SecretKey;

/**
 * Strategy interface for performing cryptographic operations on file chunks.
 * This interface abstracts the specific encryption algorithm (e.g., AES-GCM)
 * and provides the logic for chunk-level security, including nonce generation,
 * key resolution, and authentication.
 */
public interface FileStorageCryptor {

    /**
     * Generates a new metadata header for a file being prepared for encryption.
     * Implementation should generate a unique, cryptographically secure nonce prefix
     * and capture all necessary metadata (sizes, counts, key IDs) for the header.
     *
     * @param plaintextSize The total size of the unencrypted file content.
     * @param chunkSize     The size of each plaintext chunk.
     * @return A new {@link EncryptedFileHeader} instance.
     */
    EncryptedFileHeader newHeader(long plaintextSize, int chunkSize);

    /**
     * Resolves the {@link SecretKey} required to process a file based on its header.
     * This typically involves looking up a key in a registry or key management system
     * using the {@code keyId} stored within the header.
     *
     * @param header The header containing the key identifier.
     * @return The secret key used for encryption or decryption.
     * @throws FileStorageException if the key cannot be found or resolved.
     */
    SecretKey resolveKey(EncryptedFileHeader header);

    /**
     * Returns the length of the initialization vector or nonce in bytes.
     *
     * @return length of nonce
     */
    int nonceLength();

    /**
     * Returns the length of the authentication tag (e.g., GCM tag) in bytes.
     *
     * @return length of auth tag
     */
    int tagLength();

    /**
     * Calculates the total size of an encrypted chunk based on the plaintext size.
     * The default calculation is: {@code nonceLength + plaintextChunkSize + tagLength}.
     *
     * @param plaintextChunkSize The size of the raw data block.
     * @return The total size of the resulting encrypted chunk in bytes.
     */
    default int encryptedChunkSize(final int plaintextChunkSize) {
        return nonceLength() + plaintextChunkSize + tagLength();
    }

    /**
     * Encrypts a single chunk of plaintext.
     * Implementations must ensure that the resulting byte array is formatted consistently
     * for {@link #decryptChunk(SecretKey, EncryptedFileHeader, int, byte[])} to process.
     *
     * @param key        The key to use for encryption.
     * @param header     The file header providing context (e.g., nonce prefix).
     * @param chunkIndex The position of this chunk within the file.
     * @param plaintext  The source buffer containing the data to encrypt.
     * @param offset     The starting offset in the plaintext buffer.
     * @param length     The number of bytes to encrypt.
     * @return The encrypted byte array containing the nonce, ciphertext, and authentication tag.
     */
    byte[] encryptChunk(
            SecretKey key,
            EncryptedFileHeader header,
            int chunkIndex,
            byte[] plaintext,
            int offset,
            int length
    );

    /**
     * Decrypts and authenticates a single encrypted chunk.
     *
     * @param key            The key to use for decryption.
     * @param header         The file header providing context.
     * @param chunkIndex     The expected position of this chunk.
     * @param encryptedChunk The full encrypted payload (nonce + ciphertext + tag).
     * @return The decrypted plaintext bytes.
     * @throws FileStorageException if decryption or authentication fails.
     */
    byte[] decryptChunk(
            SecretKey key,
            EncryptedFileHeader header,
            int chunkIndex,
            byte[] encryptedChunk
    );
}
