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

class AesGcmChunkedFileStorageCryptorTest {

    private static final byte[] BYTE_CONTENT = secure().nextNumeric(1024).getBytes(UTF_8);
    private static final UUID FILE_ID = UUID.randomUUID();
    private static final FileStorageService<UUID> FILE_STORAGE_SERVICE;
    private static final Path ROOT_PATH;

    static {
        try {
            ROOT_PATH = Files.createTempDirectory(SystemFileStorageServiceImplTest.class.getName());
            FILE_STORAGE_SERVICE = new EncryptingFileStorageServiceWrapper<>(
                    new SystemFileStorageServiceImpl<>(UUIDFileStorageId::new, ROOT_PATH),
                    new AesGcmChunkedFileStorageCryptor("main", "V3dYQjZ2cWJ5QjVyR0t3Y0tVS2N6N0pqZGRxNzR2cVg="),
                    128
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void tearDown() {
        FILE_STORAGE_SERVICE.deleteAll();
    }

    @Test
    public void testEncryption() throws IOException {
        assertThatNoException().isThrownBy(() -> FILE_STORAGE_SERVICE.create(FILE_ID, BYTE_CONTENT));
        assertThat(FILE_STORAGE_SERVICE.exists(FILE_ID)).isTrue();
        assertThat(FILE_STORAGE_SERVICE.getBytes(FILE_ID)).isEqualTo(BYTE_CONTENT);
        assertThat(FILE_STORAGE_SERVICE.getSize(FILE_ID)).isEqualTo(BYTE_CONTENT.length);

        final long size = Files.size(
                Path.of(
                        ROOT_PATH.toString(),
                        FILE_STORAGE_SERVICE.getFileStorageIdService().fromId(FILE_ID).generatePath()
                )
        );
        assertThat(size)
                .isNotEqualTo(BYTE_CONTENT.length) // not same size as encrypted version
                .isGreaterThan(BYTE_CONTENT.length); // encrypted version should be larger

        assertThat(FILE_STORAGE_SERVICE.getBytes(FILE_ID, 100, 300))
                .isEqualTo(Arrays.copyOfRange(BYTE_CONTENT, 100, 300));
        assertThat(IOUtils.toByteArray(FILE_STORAGE_SERVICE.getInputStream(FILE_ID))).isEqualTo(BYTE_CONTENT);

        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.create(FILE_ID, BYTE_CONTENT))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to create file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File already exist");

        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.create(FILE_ID, new ByteArrayInputStream(BYTE_CONTENT), BYTE_CONTENT.length))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to create file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File already exist");

        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.create(FILE_ID, new ByteArrayInputStream(BYTE_CONTENT), -12))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("contentSize must be >= 0");

        assertThatNoException().isThrownBy(() -> FILE_STORAGE_SERVICE.delete(FILE_ID));
        assertThat(FILE_STORAGE_SERVICE.getStorageDescription()).isNotEmpty();

        assertThatNoException().isThrownBy(() -> FILE_STORAGE_SERVICE.
                create(FILE_ID, new ByteArrayInputStream(BYTE_CONTENT), BYTE_CONTENT.length));

        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.getBytes(FILE_ID, -4, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("startPosition must be >= 0");

        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.getBytes(FILE_ID, 10, 4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("endPosition must be >= startPosition");

        assertThat(FILE_STORAGE_SERVICE.getBytes(FILE_ID, 4, 4)).hasSize(0);

        assertThatNoException().isThrownBy(FILE_STORAGE_SERVICE::deleteAll);
        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.getBytes(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes from file with ID: %s", FILE_ID);

        assertThatThrownBy(() -> FILE_STORAGE_SERVICE.getSize(FILE_ID))
                .isInstanceOf(FileStorageException.class);
    }
}