package com.temesoft.fs;

import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation for file storage service using Amazon S3
 */
public class S3FileStorageServiceImpl implements FileStorageService {

    private final S3Client s3Client;
    private final String bucketName;

    /**
     * Constructor taking {@link S3Client} and as argument to set up AWS S3 file storage with provided bucket name
     */
    public S3FileStorageServiceImpl(final S3Client s3Client, final String bucketName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
    }

    /**
     * Describes the storage type of implementation
     */
    @Override
    public String getStorageDescription() {
        return "S3 file storage";
    }

    /**
     * Checks by id if file exists, and if it does - returns true
     *
     * @param id - file id
     * @throws FileStorageException - thrown when unable to verify file existence
     */
    @Override
    public boolean exists(final FileStorageId<?> id) throws FileStorageException {
        try {
            // Create a HeadObjectRequest to check if the object exists
            final HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(id.generatePath())
                    .build();
            s3Client.headObject(headObjectRequest);
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (Exception e) {
            throw new FileStorageException("Unable check existence of file with ID: " + id.value(), e);
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
    public long getSize(final FileStorageId<?> id) throws FileStorageException {
        try {
            if (doesNotExist(id)) {
                throw new IOException("File does not exist");
            }
            final HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(id.generatePath())
                    .build();
            return s3Client.headObject(headObjectRequest).contentLength();
        } catch (Exception e) {
            throw new FileStorageException("Unable to get file size with ID: " + id.value(), e);
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
    public void create(final FileStorageId<?> id, final byte[] bytes) throws FileStorageException {
        try {
            if (exists(id)) {
                throw new IOException("File already exist");
            }
            final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(id.generatePath())
                    .build();
            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(bytes));
        } catch (Exception e) {
            throw new FileStorageException("Unable to create file with ID: " + id.value(), e);
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
    public void create(final FileStorageId<?> id, final InputStream inputStream, final long contentSize) throws FileStorageException {
        try (inputStream) {
            if (exists(id)) {
                throw new IOException("File already exist");
            }
            final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(id.generatePath())
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(inputStream, contentSize));
        } catch (Exception e) {
            throw new FileStorageException("Unable to create file with ID: " + id.value(), e);
        }
    }

    /**
     * Deletes file using provided id
     *
     * @param id - file id
     * @throws FileStorageException - thrown when unable to delete file
     */
    @Override
    public void delete(final FileStorageId<?> id) throws FileStorageException {
        try {
            if (doesNotExist(id)) {
                throw new FileNotFoundException("File not found");
            }
            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(id.generatePath())
                    .build());
        } catch (Exception e) {
            throw new FileStorageException("Unable to delete file with ID: " + id.value(), e);
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
    public byte[] getBytes(final FileStorageId<?> id) throws FileStorageException {
        try {
            if (doesNotExist(id)) {
                throw new FileNotFoundException("File not found");
            }
            final GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(id.generatePath())
                    .build();
            return s3Client.getObject(request).readAllBytes();
        } catch (Exception e) {
            throw new FileStorageException("Unable to get bytes from file with ID: " + id.value(), e);
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
    public byte[] getBytes(final FileStorageId<?> id, final int startPosition, final int endPosition) throws FileStorageException {
        try {
            if (doesNotExist(id)) {
                throw new FileNotFoundException("File not found");
            }
            final GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(id.generatePath())
                    .range("bytes=" + startPosition + "-" + (endPosition - 1))
                    .build();
            return s3Client.getObject(request).readAllBytes();
        } catch (Exception e) {
            throw new FileStorageException("Unable to get bytes range from file with ID: " + id.value(), e);
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
    public InputStream getInputStream(final FileStorageId<?> id) throws FileStorageException {
        try {
            if (doesNotExist(id)) {
                throw new FileNotFoundException("File not found");
            }
            final GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(id.generatePath())
                    .build();
            return s3Client.getObject(request);
        } catch (Exception e) {
            throw new FileStorageException("Unable to get input stream from file with ID: " + id.value(), e);
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
            final ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .build();
            final SdkIterable<ListObjectsV2Response> listResponses = s3Client.listObjectsV2Paginator(listObjectsV2Request);
            listResponses.stream().forEach(listObjResponse -> {
                final List<S3Object> s3Objects = listObjResponse.contents();
                if (!s3Objects.isEmpty()) {
                    final List<ObjectIdentifier> objectIdentifiers = new ArrayList<>();
                    s3Objects.forEach(o -> objectIdentifiers.add(ObjectIdentifier.builder().key(o.key()).build()));
                    final DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                            .bucket(bucketName)
                            .delete(Delete.builder().objects(objectIdentifiers).build())
                            .build();
                    s3Client.deleteObjects(deleteObjectsRequest);
                }
            });
        } catch (Exception e) {
            throw new FileStorageException("Unable to delete all available files", e);
        }
    }
}
