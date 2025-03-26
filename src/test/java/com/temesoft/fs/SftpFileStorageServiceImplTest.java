package com.temesoft.fs;

import com.github.ksuid.Ksuid;
import org.apache.commons.io.IOUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.secure;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

@Testcontainers(disabledWithoutDocker = true)
class SftpFileStorageServiceImplTest {

    private static final byte[] BYTE_CONTENT = secure().nextAlphanumeric(128).getBytes(UTF_8);
    private static final Ksuid FILE_ID = Ksuid.newKsuid();
    private static final KsuidFileStorageId STORAGE_FILE_ID = new KsuidFileStorageId(FILE_ID);
    private static final String DOCKER_IMAGE = "jmcombs/sftp:latest";
    private static final String SFTP_HOST = "localhost";
    private static final int SFTP_PORT = 22;
    private static final String USERNAME = "test";
    private static final String PASSWORD = "password";
    private static final String SFTP_HOME = "/home/test";

    @Container
    private static final GenericContainer<?> sftpContainer = new GenericContainer<>(DockerImageName.parse(DOCKER_IMAGE))
            .withExposedPorts(SFTP_PORT)
            .withCommand(USERNAME + ":" + PASSWORD + ":::" + SFTP_HOME);


    private static FileStorageService fileStorageService;

    @BeforeAll
    public static void setup() {
        sftpContainer.start();
        final Properties props = new Properties();
        props.setProperty("StrictHostKeyChecking", "no");
        fileStorageService = new SftpFileStorageServiceImpl(
                SFTP_HOST,
                sftpContainer.getMappedPort(SFTP_PORT),
                USERNAME,
                PASSWORD,
                SFTP_HOME,
                props
        );
    }

    @AfterAll
    public static void teardown() {
        sftpContainer.stop();
    }

    @Test
    public void testSftpFileStorageService() throws IOException {
        assertThatNoException().isThrownBy(() -> fileStorageService.create(STORAGE_FILE_ID, BYTE_CONTENT));
        assertThat(fileStorageService.getBytes(STORAGE_FILE_ID)).isEqualTo(BYTE_CONTENT);

        assertThatThrownBy(() -> fileStorageService.getBytes(STORAGE_FILE_ID, 10, 20))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Method getBytes(...) by range is not implemented");

        assertThat(IOUtils.toByteArray(fileStorageService.getInputStream(STORAGE_FILE_ID))).isEqualTo(BYTE_CONTENT);

        assertThatThrownBy(() -> fileStorageService.create(STORAGE_FILE_ID, BYTE_CONTENT))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to create file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File already exist");

        assertThatNoException().isThrownBy(() -> fileStorageService.delete(STORAGE_FILE_ID));
        assertThat(fileStorageService.getStorageDescription()).isNotEmpty();

        assertThatNoException().isThrownBy(() -> fileStorageService.
                create(STORAGE_FILE_ID, new ByteArrayInputStream(BYTE_CONTENT), BYTE_CONTENT.length));

        assertThatNoException().isThrownBy(() -> fileStorageService.deleteAll());
        Assertions.assertThatThrownBy(() -> fileStorageService.getBytes(STORAGE_FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("No such file");
    }

    @Test
    public void testSftpFileStorageService_Exceptions() {
        assertThatThrownBy(() -> fileStorageService.getBytes(STORAGE_FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("No such file");

        assertThatThrownBy(() -> fileStorageService.getInputStream(STORAGE_FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get input stream from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("No such file");

        assertThatThrownBy(() -> fileStorageService.getBytes(STORAGE_FILE_ID, 10, 20))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Method getBytes(...) by range is not implemented");

        assertThatThrownBy(() -> fileStorageService.delete(STORAGE_FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to delete file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found: %s", STORAGE_FILE_ID);
    }
}