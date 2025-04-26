package com.temesoft.fs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

/**
 * Implementation for file storage service using Google Cloud Storage
 */
public class GcsFileStorageServiceImpl<T> implements FileStorageService<T> {

    private final FileStorageIdService<T> fileStorageIdService;
    private final Bucket bucket;

    /**
     * Constructor taking {@link FileStorageIdService} and {@link Storage} as arguments to
     * set up Google Cloud Storage with existing provided bucket name
     */
    public GcsFileStorageServiceImpl(final FileStorageIdService<T> fileStorageIdService,
                                     final Storage gcsStorage,
                                     final String bucketName) {
        this.fileStorageIdService = fileStorageIdService;
        Bucket bucketExisting = gcsStorage.get(bucketName);
        if (bucketExisting != null) {
            this.bucket = bucketExisting;
        } else {
            this.bucket = gcsStorage.create(BucketInfo.newBuilder(bucketName).build());
        }
    }

    /**
     * Checks by id if file exists, and if it does - returns true
     *
     * @param id - file id
     * @throws FileStorageException - thrown when unable to verify file existence
     */
    @Override
    public boolean exists(final T id) throws FileStorageException {
        try {
            return bucket.get(fileStorageIdService.fromId(id).generatePath()).exists();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Returns size of file content in bytes using provided id
     *
     * @param id - file id
     * @return - size of file in bytes
     * @throws FileStorageException - thrown when unable to get size of file
     */
    @Override
    public long getSize(final T id) throws FileStorageException {
        try {
            if (doesNotExist(id)) {
                throw new IOException("File does not exist");
            }
            return bucket.get(fileStorageIdService.fromId(id).generatePath()).getSize();
        } catch (Exception e) {
            throw new FileStorageException("Unable to get file size with ID: " + id, e);
        }
    }

    /**
     * Creates file using provided id and byte array
     *
     * @param id    - file id
     * @param bytes - byte array of content
     * @throws FileStorageException - thrown when unable to create file
     */
    @Override
    public void create(final T id, final byte[] bytes) throws FileStorageException {
        try {
            if (exists(id)) {
                throw new IOException("File already exist");
            }
            bucket.create(fileStorageIdService.fromId(id).generatePath(), bytes);
        } catch (Exception e) {
            throw new FileStorageException("Unable to create file with ID: " + id, e);
        }
    }

    /**
     * Creates file using provided id and input stream
     *
     * @param id          - file id
     * @param inputStream - input stream of content
     * @param contentSize - size of content in bytes
     * @throws FileStorageException - thrown when unable to create file
     */
    @Override
    public void create(final T id, final InputStream inputStream, final long contentSize) throws FileStorageException {
        try (inputStream) {
            if (exists(id)) {
                throw new IOException("File already exist");
            }
            bucket.create(fileStorageIdService.fromId(id).generatePath(), inputStream);
        } catch (Exception e) {
            throw new FileStorageException("Unable to create file with ID: " + id, e);
        }
    }

    /**
     * Deletes file using provided id
     *
     * @param id - file id
     * @throws FileStorageException - thrown when unable to delete file
     */
    @Override
    public void delete(final T id) throws FileStorageException {
        try {
            if (doesNotExist(id)) {
                throw new FileNotFoundException("File not found");
            }
            bucket.get(fileStorageIdService.fromId(id).generatePath()).delete();
        } catch (Exception e) {
            throw new FileStorageException("Unable to delete file with ID: " + id, e);
        }
    }

    /**
     * Returns byte array of file content using provided id
     *
     * @param id - file id
     * @return - byte array of file content
     * @throws FileStorageException - thrown when unable to get bytes of file
     */
    @Override
    public byte[] getBytes(final T id) throws FileStorageException {
        try {
            if (doesNotExist(id)) {
                throw new FileNotFoundException("File not found");
            }
            return bucket.get(fileStorageIdService.fromId(id).generatePath()).getContent();
        } catch (Exception e) {
            throw new FileStorageException("Unable to get bytes from file with ID: " + id, e);
        }
    }

    /**
     * Returns byte array range of file content using provided id, startPosition and endPosition
     *
     * @param id            - file id
     * @param startPosition - start position of byte array to return, inclusive.
     * @param endPosition   - end position of byte array to return, exclusive.
     * @return - byte array range of file content
     * @throws FileStorageException - thrown when unable to get bytes range of file
     */
    @Override
    public byte[] getBytes(final T id, final long startPosition, final long endPosition) throws FileStorageException {
        try {
            if (doesNotExist(id)) {
                throw new FileNotFoundException("File not found");
            }
            final Blob blob = bucket.get(fileStorageIdService.fromId(id).generatePath());
            final int rangeSize = (int) (endPosition - startPosition);

            // Create a channel to read from the blob
            try (ReadChannel reader = blob.reader()) {
                // Set the position to the starting byte
                reader.seek(startPosition);

                // Prepare buffer to read data
                final ByteBuffer buffer = ByteBuffer.allocate(Math.min(rangeSize, 1024 * 1024)); // 1MB chunks max
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

                int bytesRemaining = rangeSize;
                int bytesRead;

                // Read the specified range in chunks
                while (bytesRemaining > 0 && (bytesRead = reader.read(buffer)) > 0) {
                    buffer.flip();
                    final int bytesToWrite = Math.min(bytesRead, bytesRemaining);
                    outputStream.write(buffer.array(), 0, bytesToWrite);

                    buffer.clear();
                    bytesRemaining -= bytesToWrite;
                }

                return outputStream.toByteArray();
            }
        } catch (Exception e) {
            throw new FileStorageException("Unable to get bytes range from file with ID: " + id, e);
        }
    }

    /**
     * Returns input stream of file content using provided id
     *
     * @param id - file id
     * @return - input stream of file content
     * @throws FileStorageException - thrown when unable to get input stream of file
     */
    @Override
    public InputStream getInputStream(final T id) throws FileStorageException {
        try {
            if (doesNotExist(id)) {
                throw new FileNotFoundException("File not found");
            }
            return Channels.newInputStream(bucket.get(fileStorageIdService.fromId(id).generatePath()).reader());
        } catch (Exception e) {
            throw new FileStorageException("Unable to get input stream from file with ID: " + id, e);
        }
    }

    /**
     * Deletes all available files
     *
     * @throws FileStorageException - when unable to delete all files
     */
    @Override
    public void deleteAll() throws FileStorageException {
        try {
            bucket.list().streamAll().forEach(Blob::delete);
            bucket.delete();
        } catch (Exception e) {
            throw new FileStorageException("Unable to delete all available files", e);
        }
    }

    /**
     * Describes the storage type of implementation
     */
    @Override
    public String getStorageDescription() {
        return "Google Cloud Storage";
    }

    /**
     * Returns id service used in this file storage service
     */
    @Override
    public FileStorageIdService<T> getFileStorageIdService() {
        return fileStorageIdService;
    }
}
