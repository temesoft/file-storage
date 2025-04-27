package com.temesoft.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Wrapper implementation of {@link FileStorageService} class providing debug logging, taking as constructor argument
 * actual underlying {@link FileStorageService} implementation
 */
public class LoggingFileStorageServiceWrapper<T> implements FileStorageService<T> {

    private final Logger logger;
    private final FileStorageService<T> service;

    public LoggingFileStorageServiceWrapper(final FileStorageService<T> service) {
        this.service = service;
        logger = LoggerFactory.getLogger(service.getClass());
    }

    /**
     * Returns {@link FileStorageService} used in this wrapper as underlying implementation
     */
    public FileStorageService<T> getService() {
        return service;
    }

    /**
     * Checks by id if file exists, and if it does - returns true
     *
     * @param id - file id
     * @throws FileStorageException - thrown when unable to verify file existence
     */
    @Override
    public boolean exists(final T id) throws FileStorageException {
        logger.debug("exists('{} -> {}')", id, generatePath(id));
        return service.exists(id);
    }

    /**
     * Checks by id if file does not exist, and if it does - returns false
     *
     * @param id - file id
     * @throws FileStorageException - thrown when unable to verify file existence
     */
    @Override
    public boolean doesNotExist(final T id) throws FileStorageException {
        logger.debug("doesNotExist('{} -> {}')", id, generatePath(id));
        return !service.exists(id);
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
        logger.debug("getSize('{} -> {}')", id, generatePath(id));
        return service.getSize(id);
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
        logger.debug("create('{} -> {}', {} bytes)", id, generatePath(id), bytes.length);
        service.create(id, bytes);
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
        logger.debug("create('{} -> {}', {}, {})", id, generatePath(id), inputStream, contentSize);
        service.create(id, inputStream, contentSize);
    }

    /**
     * Deletes file using provided id
     *
     * @param id - file id
     * @throws FileStorageException - thrown when unable to delete file
     */
    @Override
    public void delete(final T id) throws FileStorageException {
        logger.debug("delete('{} -> {}')", id, generatePath(id));
        service.delete(id);
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
        logger.debug("getBytes('{} -> {}')", id, generatePath(id));
        return service.getBytes(id);
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
        logger.debug("getBytes('{} -> {}', {}, {})", id, generatePath(id), startPosition, endPosition);
        return service.getBytes(id, startPosition, endPosition);
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
        logger.debug("getInputStream('{} -> {}')", id, generatePath(id));
        return service.getInputStream(id);
    }

    /**
     * Deletes all available files
     *
     * @throws FileStorageException - when unable to delete all files
     */
    @Override
    public void deleteAll() throws FileStorageException {
        logger.debug("deleteAll()");
        service.deleteAll();
    }

    /**
     * Describes the storage type of implementation
     */
    @Override
    public String getStorageDescription() {
        return service.toString();
    }

    /**
     * Returns id service used in this file storage service
     */
    @Override
    public FileStorageIdService<T> getFileStorageIdService() {
        return service.getFileStorageIdService();
    }

    private String generatePath(final T id) {
        return service.getFileStorageIdService().fromId(id).generatePath();
    }
}
