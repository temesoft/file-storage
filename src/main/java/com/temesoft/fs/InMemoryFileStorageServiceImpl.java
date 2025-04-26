package com.temesoft.fs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation for file storage service using memory
 */
public class InMemoryFileStorageServiceImpl<T> implements FileStorageService<T> {

    private static final int BYTE_BUFFER_SIZE = 10_240;

    private final Map<String, byte[]> files = new ConcurrentHashMap<>();

    private final FileStorageIdService<T> fileStorageIdService;

    /**
     * Constructor taking {@link FileStorageIdService} as argument to set up in-memory file storage
     */
    public InMemoryFileStorageServiceImpl(final FileStorageIdService<T> fileStorageIdService) {
        this.fileStorageIdService = fileStorageIdService;
    }

    /**
     * Describes the storage type of implementation
     */
    @Override
    public String getStorageDescription() {
        return "In memory file storage";
    }

    /**
     * Checks by id if file exists, and if it does - returns true
     *
     * @param id - file id
     * @throws FileStorageException - thrown when unable to verify file existence
     */
    @Override
    public boolean exists(final T id) throws FileStorageException {
        return files.containsKey(fileStorageIdService.fromId(id).generatePath());
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
            return files.get(fileStorageIdService.fromId(id).generatePath()).length;
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
            files.put(fileStorageIdService.fromId(id).generatePath(), bytes);
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
            final byte[] buffer = new byte[BYTE_BUFFER_SIZE];
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            files.put(fileStorageIdService.fromId(id).generatePath(), outputStream.toByteArray());
        } catch (IOException e) {
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
            files.remove(fileStorageIdService.fromId(id).generatePath());
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
            return files.get(fileStorageIdService.fromId(id).generatePath());
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
            return Arrays.copyOfRange(files.get(fileStorageIdService.fromId(id).generatePath()), (int) startPosition, (int) endPosition);
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
            return new ByteArrayInputStream(files.get(fileStorageIdService.fromId(id).generatePath()));
        } catch (Exception e) {
            throw new FileStorageException("Unable to get input stream from file with ID: " + id, e);
        }
    }

    /**
     * Deletes all available files
     */
    @Override
    public void deleteAll() {
        files.clear();
    }

    /**
     * Returns id service used in this file storage service
     */
    @Override
    public FileStorageIdService<T> getFileStorageIdService() {
        return fileStorageIdService;
    }
}