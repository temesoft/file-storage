package com.temesoft.fs;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class EncryptingFileStorageServiceWrapperTest {

    final FileStorageService<UUID> fileStorageService = new InMemoryFileStorageServiceImpl<>(UUIDFileStorageId::new);
    final FileStorageCryptor fileStorageCryptor = new AesGcmChunkedFileStorageCryptor("test", "V3dYQjZ2cWJ5QjVyR0t3Y0tVS2N6N0pqZGRxNzR2cVg=");

    @Test
    public void testConstructor() {
        assertThatThrownBy(() -> new EncryptingFileStorageServiceWrapper(null, fileStorageCryptor, 1024))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("service must not be null");

        assertThatThrownBy(() -> new EncryptingFileStorageServiceWrapper(fileStorageService, null, 1024))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("cryptor must not be null");

        assertThatThrownBy(() -> new EncryptingFileStorageServiceWrapper(fileStorageService, fileStorageCryptor, -12))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("chunkSize must be > 0");
    }
}