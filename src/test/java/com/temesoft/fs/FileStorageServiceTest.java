package com.temesoft.fs;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.secure;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This test class validates default methods from interface {@link FileStorageService} using in-memory storage
 */
class FileStorageServiceTest {

    private static final byte[] BYTE_CONTENT_1 = secure().nextAlphanumeric(1024).getBytes(UTF_8);
    private static final byte[] BYTE_CONTENT_2 = secure().nextAlphanumeric(1024).getBytes(UTF_8);
    private static final UUID FILE_ID = UUID.randomUUID();

    @Test
    public void testDefaultMethods_DoesNotExist() {
        final FileStorageService<UUID> service = new InMemoryFileStorageServiceImpl<>(UUIDFileStorageId::new);
        assertThat(service.doesNotExist(FILE_ID))
                .isTrue()
                .isNotEqualTo(service.exists(FILE_ID));
        service.create(FILE_ID, BYTE_CONTENT_1);
        assertThat(service.doesNotExist(FILE_ID))
                .isFalse()
                .isNotEqualTo(service.exists(FILE_ID));
    }

    @Test
    public void testDefaultMethods_CreateWithOverwrite() {
        final FileStorageService<UUID> service = new InMemoryFileStorageServiceImpl<>(UUIDFileStorageId::new);
        service.create(FILE_ID, BYTE_CONTENT_1);
        service.create(FILE_ID, BYTE_CONTENT_2, true);
        assertThat(service.getBytes(FILE_ID)).isEqualTo(BYTE_CONTENT_2);

        assertThatThrownBy(() -> service.create(FILE_ID, BYTE_CONTENT_2, false))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to create file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File already exist");
    }

    @Test
    public void testDefaultMethods_CreateStreamWithOverwrite() {
        final FileStorageService<UUID> service = new InMemoryFileStorageServiceImpl<>(UUIDFileStorageId::new);
        service.create(FILE_ID, BYTE_CONTENT_1);
        service.create(FILE_ID,
                new ByteArrayInputStream(BYTE_CONTENT_2), BYTE_CONTENT_2.length, true);
        assertThat(service.getBytes(FILE_ID)).isEqualTo(BYTE_CONTENT_2);

        assertThatThrownBy(() -> service.create(FILE_ID,
                new ByteArrayInputStream(BYTE_CONTENT_2), BYTE_CONTENT_2.length, false))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to create file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File already exist");
    }

    @Test
    public void testDefaultMethods_DeleteIfExists() {
        final FileStorageService<UUID> service = new InMemoryFileStorageServiceImpl<>(UUIDFileStorageId::new);
        service.create(FILE_ID, BYTE_CONTENT_1);
        service.deleteIfExists(FILE_ID);
        assertThat(service.exists(FILE_ID)).isFalse();

        // No exception is thrown when file does not exist
        service.deleteIfExists(FILE_ID);
    }
}