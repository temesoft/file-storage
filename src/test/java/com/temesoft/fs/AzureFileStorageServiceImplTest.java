package com.temesoft.fs;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.azure.AzuriteContainer;
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
public class AzureFileStorageServiceImplTest {

    private static final byte[] BYTE_CONTENT = secure().nextAlphanumeric(1024).getBytes(UTF_8);
    private static final UUID FILE_ID = UUID.randomUUID();
    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://%s:%d/devstoreaccount1;";
    private static final String CONTAINER_NAME = "my-container";
    private static final String DOCKER_IMAGE = "mcr.microsoft.com/azure-storage/azurite";

    @Container
    private static final GenericContainer<?> azurite = new AzuriteContainer(DockerImageName.parse(DOCKER_IMAGE))
            .withExposedPorts(10000);

    private static FileStorageService<UUID> fileStorageService;

    @BeforeAll
    public static void setup() {
        final String connectionString = String.format(CONNECTION_STRING, azurite.getHost(), azurite.getFirstMappedPort());
        final BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();

        fileStorageService = new AzureFileStorageServiceImpl<>(
                UUIDFileStorageId::new,
                blobServiceClient.getBlobContainerClient(CONTAINER_NAME)
        );
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

        assertThatThrownBy(() -> fileStorageService.delete(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to delete file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found");
    }
}
