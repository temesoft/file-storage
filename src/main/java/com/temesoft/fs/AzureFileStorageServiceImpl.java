package com.temesoft.fs;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.options.BlobInputStreamOptions;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation for file storage service using Azure cloud storage
 */
public class AzureFileStorageServiceImpl<T> implements FileStorageService<T> {

    private static final int BYTE_BUFFER_SIZE = 10_240;

    private final FileStorageIdService<T> fileStorageIdService;
    private final BlobContainerClient containerClient;

    /**
     * Constructor taking {@link FileStorageIdService} and {@link BlobContainerClient} as arguments to
     * set up Azure blob storage
     */
    public AzureFileStorageServiceImpl(final FileStorageIdService<T> fileStorageIdService,
                                       final BlobContainerClient containerClient) {
        this.fileStorageIdService = fileStorageIdService;
        this.containerClient = containerClient;
        if (!containerClient.exists()) {
            containerClient.create();
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
            return containerClient.getBlobClient(fileStorageIdService.fromId(id).generatePath()).exists();
        } catch (Exception e) {
            throw new FileStorageException("Unable check existence of file with ID: " + id, e);
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
            return containerClient.getBlobClient(fileStorageIdService.fromId(id).generatePath()).getProperties().getBlobSize();
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
            containerClient.getBlobClient(fileStorageIdService.fromId(id).generatePath())
                    .upload(BinaryData.fromBytes(bytes));
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
            containerClient.getBlobClient(fileStorageIdService.fromId(id).generatePath())
                    .upload(inputStream, contentSize);
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
            containerClient.getBlobClient(fileStorageIdService.fromId(id).generatePath())
                    .delete();
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
            return containerClient.getBlobClient(fileStorageIdService.fromId(id).generatePath())
                    .downloadContent().toBytes();
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
            final BlobClient blobClient = containerClient.getBlobClient(fileStorageIdService.fromId(id).generatePath());
            final BlobRange blobRange = new BlobRange(startPosition, endPosition - startPosition);
            final BlobInputStreamOptions options = new BlobInputStreamOptions().setRange(blobRange);
            try (InputStream inputStream = blobClient.openInputStream(options)) {
                final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                final byte[] data = new byte[BYTE_BUFFER_SIZE];
                int nRead;
                while ((nRead = inputStream.read(data, 0, data.length)) != -1) {
                    buffer.write(data, 0, nRead);
                }

                buffer.flush();
                return buffer.toByteArray();
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
            return containerClient.getBlobClient(fileStorageIdService.fromId(id).generatePath()).openInputStream();
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
            containerClient.delete();
        } catch (Exception e) {
            throw new FileStorageException("Unable to delete all available files", e);
        }
    }

    /**
     * Describes the storage type of implementation
     */
    @Override
    public String getStorageDescription() {
        return "Azure Storage";
    }

    /**
     * Returns id service used in this file storage service
     */
    @Override
    public FileStorageIdService<T> getFileStorageIdService() {
        return fileStorageIdService;
    }
}

