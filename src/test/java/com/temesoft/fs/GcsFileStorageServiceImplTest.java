package com.temesoft.fs;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.secure;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers(disabledWithoutDocker = true)
class GcsFileStorageServiceImplTest {

    private static final byte[] BYTE_CONTENT = secure().nextAlphanumeric(1024).getBytes(UTF_8);
    private static final String BUCKET = "test-bucket-1";
    private static final UUID FILE_ID = UUID.randomUUID();
    private static final String DOCKER_IMAGE = "fsouza/fake-gcs-server";

    @Container
    private static final GenericContainer<?> GCS_CONTAINER = new GenericContainer<>(DockerImageName.parse(DOCKER_IMAGE))
            .withExposedPorts(4443)
            .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
                    "/bin/fake-gcs-server",
                    "-scheme", "http"
            ));

    private static Storage gcsStorage;
    private static FileStorageService<UUID> fileStorageService;

    @BeforeAll
    public static void setup() {
        final Storage gcsStorage = StorageOptions.newBuilder()
                .setHost("http://localhost:" + GCS_CONTAINER.getMappedPort(4443))
                .setProjectId("test-project")
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getService();
        fileStorageService = new LoggingFileStorageServiceWrapper<>(
                new GcsFileStorageServiceImpl<>(UUIDFileStorageId::new, gcsStorage, BUCKET));
    }

    @AfterAll
    public static void cleanup() throws Exception {
        if (gcsStorage != null) {
            gcsStorage.close();
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

        assertThatNoException().isThrownBy(() -> fileStorageService.delete(FILE_ID));
        assertThat(fileStorageService.getStorageDescription()).isNotEmpty();

        assertThatNoException().isThrownBy(() -> fileStorageService.
                create(FILE_ID, new ByteArrayInputStream(BYTE_CONTENT), BYTE_CONTENT.length));

        assertThatThrownBy(() -> fileStorageService.create(FILE_ID, BYTE_CONTENT))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to create file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File already exist");

        assertThatThrownBy(() -> fileStorageService.create(FILE_ID, new ByteArrayInputStream(BYTE_CONTENT), BYTE_CONTENT.length))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to create file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File already exist");

        assertThatNoException().isThrownBy(() -> fileStorageService.deleteAll());
        assertThatThrownBy(() -> fileStorageService.getBytes(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found");
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

        assertThatThrownBy(() -> fileStorageService.getSize(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get file size with ID: %s", FILE_ID)
                .hasRootCauseMessage("File does not exist");

        assertThatThrownBy(() -> fileStorageService.delete(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to delete file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found");
    }
}
