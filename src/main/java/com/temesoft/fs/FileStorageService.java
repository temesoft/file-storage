package com.temesoft.fs;

import java.io.InputStream;

/**
 * Interface for file storage service
 */
public interface FileStorageService<T> {

    /**
     * Checks by id if file exists, and if it does - returns true
     *
     * @param id - file id
     * @throws FileStorageException - thrown when unable to verify file existence
     */
    boolean exists(T id) throws FileStorageException;

    /**
     * Checks by id if file does not exist, and if it does - returns false
     *
     * @param id - file id
     * @throws FileStorageException - thrown when unable to verify file existence
     */
    default boolean doesNotExist(T id) throws FileStorageException {
        return !exists(id);
    }

    /**
     * Returns size of file content in bytes using provided id
     *
     * @param id - file id
     * @return - size of file in bytes
     * @throws FileStorageException - thrown when unable to get size of file
     */
    long getSize(T id) throws FileStorageException;

    /**
     * Creates file using provided id and byte array
     *
     * @param id    - file id
     * @param bytes - byte array of content
     * @throws FileStorageException - thrown when unable to create file
     */
    void create(T id, byte[] bytes) throws FileStorageException;

    /**
     * Creates file using provided id and input stream
     *
     * @param id          - file id
     * @param inputStream - input stream of content
     * @param contentSize - size of content in bytes
     * @throws FileStorageException - thrown when unable to create file
     */
    void create(T id, InputStream inputStream, long contentSize) throws FileStorageException;

    /**
     * Deletes file using provided id
     *
     * @param id - file id
     * @throws FileStorageException - thrown when unable to delete file
     */
    void delete(T id) throws FileStorageException;

    /**
     * Returns byte array of file content using provided id
     *
     * @param id - file id
     * @return - byte array of file content
     * @throws FileStorageException - thrown when unable to get bytes of file
     */
    byte[] getBytes(T id) throws FileStorageException;

    /**
     * Returns byte array range of file content using provided id, startPosition and endPosition
     *
     * @param id            - file id
     * @param startPosition - start position of byte array to return, inclusive.
     * @param endPosition   - end position of byte array to return, exclusive.
     * @return - byte array range of file content
     * @throws FileStorageException - thrown when unable to get bytes range of file
     */
    byte[] getBytes(T id, long startPosition, long endPosition) throws FileStorageException;

    /**
     * Returns input stream of file content using provided id
     *
     * @param id - file id
     * @return - input stream of file content
     * @throws FileStorageException - thrown when unable to get input stream of file
     */
    InputStream getInputStream(T id) throws FileStorageException;

    /**
     * Deletes all available files
     *
     * @throws FileStorageException - when unable to delete all files
     */
    void deleteAll() throws FileStorageException;

    /**
     * Describes the storage type of implementation
     */
    String getStorageDescription();

    /**
     * Returns id service used in this file storage service
     */
    FileStorageIdService<T> getFileStorageIdService();

}