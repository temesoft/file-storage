package com.temesoft.fs;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.secure;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

@Testcontainers(disabledWithoutDocker = true)
class S3FileStorageServiceImplTest {

    private static final String BUCKET_NAME = secure().nextAlphanumeric(20).toLowerCase();
    private static final byte[] BYTE_CONTENT = secure().nextAlphanumeric(1024).getBytes(UTF_8);
    private static final UUID FILE_ID = UUID.randomUUID();

    @Container
    private static final LocalStackContainer localstack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:latest"))
            .withServices(S3);

    private static S3Client s3Client;
    private static FileStorageService<UUID> fileStorageService;

    @BeforeAll
    public static void setup() {
        // Configure S3 client to point to the localstack container
        s3Client = S3Client.builder()
                .endpointOverride(localstack.getEndpointOverride(S3))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
                ))
                .region(Region.of(localstack.getRegion()))
                .build();

        // Create test bucket
        s3Client.createBucket(CreateBucketRequest.builder()
                .bucket(BUCKET_NAME)
                .build());

        fileStorageService = new LoggingFileStorageServiceWrapper<>(
                new S3FileStorageServiceImpl<>(UUIDFileStorageId::new, s3Client, BUCKET_NAME));
    }

    @AfterAll
    public static void cleanup() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    @Test
    public void testFileStorageService() throws IOException {
        assertThatNoException().isThrownBy(() -> fileStorageService.create(FILE_ID, BYTE_CONTENT));
        assertThat(fileStorageService.getBytes(FILE_ID)).isEqualTo(BYTE_CONTENT);
        assertThat(fileStorageService.getSize(FILE_ID)).isEqualTo(BYTE_CONTENT.length);
        assertThat(fileStorageService.getBytes(FILE_ID, 10, 20))
                .isEqualTo(Arrays.copyOfRange(BYTE_CONTENT, 10, 20));
        assertThat(IOUtils.toByteArray(fileStorageService.getInputStream(FILE_ID))).isEqualTo(BYTE_CONTENT);

        assertThatThrownBy(() -> fileStorageService.create(FILE_ID, BYTE_CONTENT))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to create file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File already exist");

        assertThatThrownBy(() -> fileStorageService.create(FILE_ID, new ByteArrayInputStream(BYTE_CONTENT), BYTE_CONTENT.length))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to create file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File already exist");

        assertThatNoException().isThrownBy(() -> fileStorageService.delete(FILE_ID));
        assertThat(fileStorageService.getStorageDescription()).isNotEmpty();

        assertThatNoException().isThrownBy(() -> fileStorageService.
                create(FILE_ID, new ByteArrayInputStream(BYTE_CONTENT), BYTE_CONTENT.length));

        assertThatNoException().isThrownBy(() -> fileStorageService.deleteAll());
        assertThatThrownBy(() -> fileStorageService.getBytes(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found");

        assertThatThrownBy(() -> fileStorageService.getSize(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get file size with ID: %s", FILE_ID);
    }

    @Test
    public void testFileStorageService_Exceptions() {
        assertThatThrownBy(() -> fileStorageService.getBytes(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found");

        assertThatThrownBy(() -> fileStorageService.getInputStream(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get input stream from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found");

        assertThatThrownBy(() -> fileStorageService.getBytes(FILE_ID, 10, 20))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes range from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found");

        assertThatThrownBy(() -> fileStorageService.delete(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to delete file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found");
    }
}