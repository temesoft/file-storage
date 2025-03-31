package com.temesoft.fs;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.secure;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SystemFileStorageServiceImplTest {

    private static final byte[] BYTE_CONTENT = secure().nextAlphanumeric(1024).getBytes(UTF_8);
    private static final UUID FILE_ID = UUID.randomUUID();
    private static final UUIDFileStorageId STORAGE_FILE_ID = new UUIDFileStorageId(FILE_ID);
    private static final FileStorageService FILE_STORAGE_SERVICE;
    private static final Path ROOT_PATH;

    static {
        try {
            ROOT_PATH = Files.createTempDirectory(SystemFileStorageServiceImplTest.class.getName());
            FILE_STORAGE_SERVICE = new SystemFileStorageServiceImpl(ROOT_PATH);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void tearDown() {
        FILE_STORAGE_SERVICE.deleteAll();
    }

    @Test
    public void testDefaultFileStorageService() throws IOException {
        assertThatNoException().isThrownBy(() -> FILE_STORAGE_SERVICE.create(STORAGE_FILE_ID, BYTE_CONTENT));
        assertThat(FILE_STORAGE_SERVICE.getBytes(STORAGE_FILE_ID)).isEqualTo(BYTE_CONTENT);
        assertThat(FILE_STORAGE_SERVICE.getSize(STORAGE_FILE_ID)).isEqualTo(BYTE_CONTENT.length);

        assertThat(FILE_STORAGE_SERVICE.getBytes(STORAGE_FILE_ID, 10, 20))
                .isEqualTo(Arrays.copyOfRange(BYTE_CONTENT, 10, 20));
        assertThat(IOUtils.toByteArray(FILE_STORAGE_SERVICE.getInputStream(STORAGE_FILE_ID))).isEqualTo(BYTE_CONTENT);

        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.create(STORAGE_FILE_ID, BYTE_CONTENT))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to create file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File already exist");

        assertThatNoException().isThrownBy(() -> FILE_STORAGE_SERVICE.delete(STORAGE_FILE_ID));
        assertThat(FILE_STORAGE_SERVICE.getStorageDescription()).isNotEmpty();

        assertThatNoException().isThrownBy(() -> FILE_STORAGE_SERVICE.
                create(STORAGE_FILE_ID, new ByteArrayInputStream(BYTE_CONTENT), BYTE_CONTENT.length));

        assertThatNoException().isThrownBy(FILE_STORAGE_SERVICE::deleteAll);
        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.getBytes(STORAGE_FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes from file with ID: %s", FILE_ID);
    }

    @Test
    public void testDefaultFileStorageService_Exceptions() {
        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.getBytes(STORAGE_FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found: %s", ROOT_PATH.resolve(STORAGE_FILE_ID.generatePath()));

        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.getInputStream(STORAGE_FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get input stream from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found: %s", ROOT_PATH.resolve(STORAGE_FILE_ID.generatePath()));

        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.getBytes(STORAGE_FILE_ID, 10, 20))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes range from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found: %s", ROOT_PATH.resolve(STORAGE_FILE_ID.generatePath()));

        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.delete(STORAGE_FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to delete file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found: %s", ROOT_PATH.resolve(STORAGE_FILE_ID.generatePath()));
    }
}