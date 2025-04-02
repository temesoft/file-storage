package com.temesoft.fs;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.secure;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryFileStorageServiceImplTest {

    private static final byte[] BYTE_CONTENT = secure().nextAlphanumeric(1024).getBytes(UTF_8);
    private static final UUID FILE_ID = UUID.randomUUID();
    private static final FileStorageService<UUID> FILE_STORAGE_SERVICE = new InMemoryFileStorageServiceImpl<>(UUIDFileStorageId::new);

    @AfterEach
    public void tearDown() {
        FILE_STORAGE_SERVICE.deleteAll();
    }

    @Test
    public void testFileStorageService() throws IOException {
        assertThatNoException().isThrownBy(() -> FILE_STORAGE_SERVICE.create(FILE_ID, BYTE_CONTENT));
        assertThat(FILE_STORAGE_SERVICE.getBytes(FILE_ID)).isEqualTo(BYTE_CONTENT);
        assertThat(FILE_STORAGE_SERVICE.getSize(FILE_ID)).isEqualTo(BYTE_CONTENT.length);
        assertThat(FILE_STORAGE_SERVICE.getBytes(FILE_ID, 10, 20))
                .isEqualTo(Arrays.copyOfRange(BYTE_CONTENT, 10, 20));
        assertThat(IOUtils.toByteArray(FILE_STORAGE_SERVICE.getInputStream(FILE_ID))).isEqualTo(BYTE_CONTENT);

        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.create(FILE_ID, BYTE_CONTENT))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to create file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File already exist");

        assertThatNoException().isThrownBy(() -> FILE_STORAGE_SERVICE.delete(FILE_ID));
        assertThat(FILE_STORAGE_SERVICE.getStorageDescription()).isNotEmpty();

        assertThatNoException().isThrownBy(() -> FILE_STORAGE_SERVICE.
                create(FILE_ID, new ByteArrayInputStream(BYTE_CONTENT), BYTE_CONTENT.length));

        assertThatNoException().isThrownBy(FILE_STORAGE_SERVICE::deleteAll);
        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.getBytes(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found");
    }

    @Test
    public void testFileStorageService_Exceptions() {
        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.getBytes(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found");

        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.getInputStream(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get input stream from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found");

        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.getBytes(FILE_ID, 10, 20))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes range from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found");

        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.delete(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to delete file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found");
    }
}