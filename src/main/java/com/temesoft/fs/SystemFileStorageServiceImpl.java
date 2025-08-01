package com.temesoft.fs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Implementation for file storage service using file system (java.nio)
 */
public class SystemFileStorageServiceImpl<T> implements FileStorageService<T> {

    private final FileStorageIdService<T> fileStorageIdService;
    private final Path rootPath;

    /**
     * Constructor taking {@link FileStorageIdService} and {@link Path} as argument to set up system file storage with provided root
     */
    public SystemFileStorageServiceImpl(final FileStorageIdService<T> fileStorageIdService, final Path rootPath) {
        this.fileStorageIdService = fileStorageIdService;
        this.rootPath = rootPath;
        try {
            Files.createDirectories(rootPath);
        } catch (IOException e) {
            throw new FileStorageException("Unable to create directory: " + rootPath, e);
        }
    }

    /**
     * Describes the storage type of implementation
     */
    @Override
    public String getStorageDescription() {
        return "File system storage";
    }

    /**
     * Checks by id if file exists, and if it does - returns true
     *
     * @param id - file id
     * @throws FileStorageException - thrown when unable to verify file existence
     */
    @Override
    public boolean exists(final T id) throws FileStorageException {
        return Files.exists(rootPath.resolve(fileStorageIdService.fromId(id).generatePath()));
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
            return Files.size(rootPath.resolve(fileStorageIdService.fromId(id).generatePath()));
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
            final Path path = rootPath.resolve(fileStorageIdService.fromId(id).generatePath());
            Files.createDirectories(path.getParent());
            Files.write(path, bytes);
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
            final Path path = rootPath.resolve(fileStorageIdService.fromId(id).generatePath());
            Files.createDirectories(path.getParent());
            Files.copy(inputStream, path);
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
            final Path path = rootPath.resolve(fileStorageIdService.fromId(id).generatePath());
            if (doesNotExist(id)) {
                throw new FileNotFoundException("File not found: " + path);
            }
            Files.delete(path);
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
            final Path path = rootPath.resolve(fileStorageIdService.fromId(id).generatePath());
            if (doesNotExist(id)) {
                throw new FileNotFoundException("File not found: " + path);
            }
            return Files.readAllBytes(path);
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
            final Path path = rootPath.resolve(fileStorageIdService.fromId(id).generatePath());
            if (doesNotExist(id)) {
                throw new FileNotFoundException("File not found: " + path);
            }
            try (final RandomAccessFile randomAccessFile = new RandomAccessFile(path.toFile(), "r")) {
                final byte[] buffer = new byte[(int) (endPosition - startPosition)];
                randomAccessFile.seek(startPosition);
                randomAccessFile.readFully(buffer);
                return buffer;
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
            final Path path = rootPath.resolve(fileStorageIdService.fromId(id).generatePath());
            if (doesNotExist(id)) {
                throw new FileNotFoundException("File not found: " + path);
            }
            return new FileInputStream(path.toFile());
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
    public void deleteAll() {
        try {
            deleteRecursively(rootPath);
        } catch (IOException e) {
            throw new FileStorageException("Unable to delete all available files", e);
        }
    }

    /**
     * Returns id service used in this file storage service
     */
    @Override
    public FileStorageIdService<T> getFileStorageIdService() {
        return fileStorageIdService;
    }

    private void deleteRecursively(final Path root) throws IOException {
        if (root != null && Files.exists(root)) {
            Files.walkFileTree(root, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }
}