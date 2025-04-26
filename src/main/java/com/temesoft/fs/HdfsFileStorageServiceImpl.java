package com.temesoft.fs;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation for file storage service using Apache HDFS
 */
public class HdfsFileStorageServiceImpl<T> implements FileStorageService<T> {

    private static final int BYTE_BUFFER_SIZE = 10_240;

    private final FileStorageIdService<T> fileStorageIdService;
    private final FileSystem hdfs;

    /**
     * Constructor taking {@link FileStorageIdService} and {@link FileSystem} as argument to set up HDFS file storage
     */
    public HdfsFileStorageServiceImpl(final FileStorageIdService<T> fileStorageIdService, final FileSystem hdfs) {
        this.fileStorageIdService = fileStorageIdService;
        this.hdfs = hdfs;
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
            return hdfs.exists(new Path(fileStorageIdService.fromId(id).generatePath()));
        } catch (IOException e) {
            throw new FileStorageException("Unable to verify file existence with ID: " + id, e);
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
            return hdfs.getFileStatus(new Path(fileStorageIdService.fromId(id).generatePath())).getLen();
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
            try (FSDataOutputStream outputStream = hdfs.create(new Path(fileStorageIdService.fromId(id).generatePath()))) {
                outputStream.write(bytes);
                outputStream.hflush();
            }
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
        try {
            if (exists(id)) {
                throw new IOException("File already exist");
            }
            try (FSDataOutputStream outputStream = hdfs.create(new Path(fileStorageIdService.fromId(id).generatePath()))) {
                final byte[] buffer = new byte[BYTE_BUFFER_SIZE];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
                outputStream.hflush();
            }
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
            final String path = fileStorageIdService.fromId(id).generatePath();
            if (!exists(id)) {
                throw new FileNotFoundException("File not found: " + path);
            }
            hdfs.delete(new Path(path), false);
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
            final String path = fileStorageIdService.fromId(id).generatePath();
            if (!exists(id)) {
                throw new FileNotFoundException("File not found: " + path);
            }
            try (FSDataInputStream inputStream = hdfs.open(new Path(path))) {
                return IOUtils.toByteArray(inputStream);
            }
        } catch (IOException e) {
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
            try (FSDataInputStream inputStream = hdfs.open(new Path(fileStorageIdService.fromId(id).generatePath()))) {
                inputStream.seek(startPosition);
                final int length = (int) (endPosition - startPosition);
                final byte[] buffer = new byte[length];

                // Read the specified number of bytes
                final int bytesRead = inputStream.read(buffer, 0, length);

                // If fewer bytes were read than requested, trim the array
                if (bytesRead < length) {
                    final byte[] trimmedBuffer = new byte[bytesRead];
                    System.arraycopy(buffer, 0, trimmedBuffer, 0, bytesRead);
                    return trimmedBuffer;
                }
                return buffer;
            }
        } catch (IOException e) {
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
            return hdfs.open(new Path(fileStorageIdService.fromId(id).generatePath()));
        } catch (IOException e) {
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
            hdfs.delete(hdfs.getHomeDirectory(), true);
        } catch (IOException e) {
            throw new FileStorageException("Unable to delete all available files", e);
        }
    }

    /**
     * Describes the storage type of implementation
     */
    @Override
    public String getStorageDescription() {
        return "HDFS storage";
    }

    /**
     * Returns id service used in this file storage service
     */
    @Override
    public FileStorageIdService<T> getFileStorageIdService() {
        return fileStorageIdService;
    }
}
