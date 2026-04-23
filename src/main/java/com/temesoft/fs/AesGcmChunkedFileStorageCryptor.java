package com.temesoft.fs;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

/**
 * Implementation of {@link FileStorageCryptor} that provides authenticated encryption
 * using the AES/GCM algorithm in a chunked manner.
 * <p>
 * This class handles the cryptographic operations for files by splitting them into
 * manageable chunks. Each chunk is encrypted with a unique nonce derived from a
 * file-level prefix and the chunk index. To prevent chunk swapping or header
 * manipulation attacks, the file metadata and chunk index are included as
 * Additional Authenticated Data (AAD).
 * <p>
 * The output format for each chunk is: {@code [12-byte Nonce][Ciphertext][16-byte Tag]}.
 */
public final class AesGcmChunkedFileStorageCryptor implements FileStorageCryptor {

    public static final String AES = "AES";
    public static final String AES_GCM_NO_PADDING = "AES/GCM/NoPadding";
    public static final int NONCE_LENGTH = 12;
    public static final int NONCE_PREFIX_LENGTH = 8;
    public static final int TAG_LENGTH_BYTES = 16;
    public static final int TAG_LENGTH_BITS = TAG_LENGTH_BYTES * 8;

    private final SecureRandom secureRandom = new SecureRandom();
    private final Map<String, SecretKey> keysById;
    private final String defaultKeyId;

    public AesGcmChunkedFileStorageCryptor(final String defaultKeyId, final String base64EncodedAesKey) {
        this(defaultKeyId, Map.of(defaultKeyId, decodeAesKey(base64EncodedAesKey)));
    }

    public AesGcmChunkedFileStorageCryptor(final String defaultKeyId, final Map<String, SecretKey> keysById) {
        this.defaultKeyId = Objects.requireNonNull(defaultKeyId, "defaultKeyId must not be null");
        this.keysById = Map.copyOf(Objects.requireNonNull(keysById, "keysById must not be null"));
        if (!this.keysById.containsKey(defaultKeyId)) {
            throw new IllegalArgumentException("keysById must contain defaultKeyId");
        }
    }

    /**
     * Creates a new encryption header for a file, generating a unique random nonce prefix.
     *
     * @param plaintextSize Total size of the unencrypted file content
     * @param chunkSize     The size of each plaintext block (except potentially the last one)
     * @return A new {@link EncryptedFileHeader} containing the metadata needed for encryption/decryption
     */
    @Override
    public EncryptedFileHeader newHeader(final long plaintextSize, final int chunkSize) {
        final byte[] noncePrefix = new byte[NONCE_PREFIX_LENGTH];
        secureRandom.nextBytes(noncePrefix);

        final int chunkCount = plaintextSize == 0
                ? 0
                : Math.toIntExact((plaintextSize + chunkSize - 1L) / chunkSize);

        return new EncryptedFileHeader(
                EncryptedFileHeader.VERSION_1,
                EncryptedFileHeader.ALG_AES_GCM_CHUNKED,
                chunkSize,
                plaintextSize,
                chunkCount,
                noncePrefix,
                defaultKeyId
        );
    }

    /**
     * Retrieves the secret key associated with the ID specified in the file header.
     *
     * @param header The file header containing the {@code keyId}
     * @return The {@link SecretKey} required to process the file
     * @throws FileStorageException if the key ID is not found in the configured key map
     */
    @Override
    public SecretKey resolveKey(final EncryptedFileHeader header) {
        final SecretKey key = keysById.get(header.keyId());
        if (key == null) {
            throw new FileStorageException("No encryption key configured for keyId='" + header.keyId() + "'");
        }
        return key;
    }

    /**
     * @return The required nonce length (12 bytes) for AES-GCM
     */
    @Override
    public int nonceLength() {
        return NONCE_LENGTH;
    }

    /**
     * @return The length of the authentication tag (16 bytes) produced by AES-GCM
     */
    @Override
    public int tagLength() {
        return TAG_LENGTH_BYTES;
    }

    /**
     * Encrypts a specific block of data.
     * Derives a unique nonce for the chunk and includes file metadata in the AAD
     * to ensure data integrity and prevent chunk reordering.
     *
     * @param key        The secret key to use for encryption
     * @param header     The file header containing the nonce prefix and metadata
     * @param chunkIndex The zero-based index of the chunk in the file
     * @param plaintext  The buffer containing the data to encrypt
     * @param offset     The start position in the plaintext buffer
     * @param length     The number of bytes to encrypt
     * @return A byte array containing the nonce followed by the ciphertext and tag
     * @throws FileStorageException if encryption fails
     */
    @Override
    public byte[] encryptChunk(
            final SecretKey key,
            final EncryptedFileHeader header,
            final int chunkIndex,
            final byte[] plaintext,
            final int offset,
            final int length
    ) {
        try {
            final byte[] nonce = nonce(header.fileNoncePrefix(), chunkIndex);
            final Cipher cipher = Cipher.getInstance(AES_GCM_NO_PADDING);
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_LENGTH_BITS, nonce));
            cipher.updateAAD(buildAad(header, chunkIndex));
            final byte[] cipherTextAndTag = cipher.doFinal(plaintext, offset, length);

            final ByteBuffer output = ByteBuffer.allocate(nonce.length + cipherTextAndTag.length);
            output.put(nonce);
            output.put(cipherTextAndTag);
            return output.array();
        } catch (final Exception e) {
            throw new FileStorageException("Unable to encrypt chunk " + chunkIndex, e);
        }
    }

    /**
     * Decrypts a specific block of data.
     * This method validates that the nonce stored in the encrypted chunk matches the expected
     * nonce derived from the header and index. It also verifies the authentication tag.
     *
     * @param key            The secret key to use for decryption
     * @param header         The file header for the file being decrypted
     * @param chunkIndex     The expected zero-based index of the chunk
     * @param encryptedChunk The full encrypted chunk (nonce + ciphertext + tag)
     * @return The decrypted plaintext byte array
     * @throws FileStorageException if the chunk is malformed, the nonce mismatches, or decryption fails
     */
    @Override
    public byte[] decryptChunk(
            final SecretKey key,
            final EncryptedFileHeader header,
            final int chunkIndex,
            final byte[] encryptedChunk
    ) {
        try {
            if (encryptedChunk.length < NONCE_LENGTH + TAG_LENGTH_BYTES) {
                throw new FileStorageException("Encrypted chunk is too short");
            }

            final ByteBuffer input = ByteBuffer.wrap(encryptedChunk);
            final byte[] nonce = new byte[NONCE_LENGTH];
            input.get(nonce);

            final byte[] expected = nonce(header.fileNoncePrefix(), chunkIndex);
            if (!Arrays.equals(nonce, expected)) {
                throw new FileStorageException("Nonce mismatch for chunk " + chunkIndex);
            }

            final byte[] ciphertextAndTag = new byte[input.remaining()];
            input.get(ciphertextAndTag);

            final Cipher cipher = Cipher.getInstance(AES_GCM_NO_PADDING);
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_LENGTH_BITS, nonce));
            cipher.updateAAD(buildAad(header, chunkIndex));
            return cipher.doFinal(ciphertextAndTag);
        } catch (final FileStorageException e) {
            throw e;
        } catch (final Exception e) {
            throw new FileStorageException("Unable to decrypt chunk " + chunkIndex, e);
        }
    }

    /**
     * Decodes a Base64 string into an AES SecretKey.
     *
     * @param base64EncodedAesKey The Base64 encoded raw key bytes
     * @return A valid {@link SecretKey}
     * @throws IllegalArgumentException if the key is null or not a valid AES length (128, 192, or 256 bits)
     */
    public static SecretKey decodeAesKey(final String base64EncodedAesKey) {
        final byte[] keyBytes = Base64.getDecoder().decode(
                Objects.requireNonNull(base64EncodedAesKey, "base64EncodedAesKey must not be null")
        );
        final int bitLength = keyBytes.length * 8;
        if (bitLength != 128 && bitLength != 192 && bitLength != 256) {
            throw new IllegalArgumentException("AES key must be 128, 192, or 256 bits");
        }
        return new SecretKeySpec(keyBytes, AES);
    }

    /**
     * Derives a unique 12-byte nonce for a specific chunk.
     * The nonce is constructed by concatenating the 8-byte file-level prefix
     * with the 4-byte chunk index. This ensures that every chunk within a file
     * uses a unique initialization vector (IV) for the AES-GCM cipher.
     *
     * @param fileNoncePrefix The 8-byte random prefix unique to the file
     * @param chunkIndex      The index of the current chunk
     * @return A 12-byte nonce array
     * @throws IllegalArgumentException if the prefix length is not exactly 8 bytes
     */
    private static byte[] nonce(final byte[] fileNoncePrefix, final int chunkIndex) {
        if (fileNoncePrefix.length != NONCE_PREFIX_LENGTH) {
            throw new IllegalArgumentException("fileNoncePrefix must be 8 bytes");
        }
        final ByteBuffer nonce = ByteBuffer.allocate(NONCE_LENGTH);
        nonce.put(fileNoncePrefix);
        nonce.putInt(chunkIndex);
        return nonce.array();
    }

    /**
     * Constructs the Additional Authenticated Data (AAD) for a specific chunk.
     * The AAD binds the ciphertext to the file's metadata and the specific chunk position.
     * If an attacker attempts to swap chunks between files, reorder chunks within a file,
     * or modify the header (like changing the {@code plaintextSize}), the GCM
     * authentication check will fail during decryption.
     *
     * @param header     The file header containing the file-wide metadata
     * @param chunkIndex The index of the chunk being processed
     * @return A byte array representing the serialized AAD
     */
    private static byte[] buildAad(final EncryptedFileHeader header, final int chunkIndex) {
        final byte[] keyIdBytes = header.keyId().getBytes(StandardCharsets.UTF_8);
        final ByteBuffer aad = ByteBuffer.allocate(
                4 + 1 + 1 + 4 + 8 + 4 + 8 + 4 + keyIdBytes.length
        );
        aad.put(EncryptedFileHeader.MAGIC);
        aad.put(header.version());
        aad.put(header.algorithm());
        aad.putInt(header.chunkSize());
        aad.putLong(header.plaintextSize());
        aad.putInt(header.chunkCount());
        aad.put(header.fileNoncePrefix());
        aad.putInt(chunkIndex);
        aad.put(keyIdBytes);
        return aad.array();
    }
}
