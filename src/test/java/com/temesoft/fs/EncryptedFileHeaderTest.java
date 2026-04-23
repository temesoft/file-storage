package com.temesoft.fs;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EncryptedFileHeaderTest {

    @Test
    public void testConstructor() {
        assertThatThrownBy(() -> new EncryptedFileHeader((byte) 2, (byte) 2, -4, -4, -4, null, "test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported header version: 2");

        assertThatThrownBy(() -> new EncryptedFileHeader((byte) 1, (byte) 2, -4, -4, -4, null, "test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported algorithm: 2");

        assertThatThrownBy(() -> new EncryptedFileHeader((byte) 1, (byte) 1, -4, -4, -4, null, "test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("chunkSize must be > 0");

        assertThatThrownBy(() -> new EncryptedFileHeader((byte) 1, (byte) 1, 4, -4, -4, null, "test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("plaintextSize must be >= 0");

        assertThatThrownBy(() -> new EncryptedFileHeader((byte) 1, (byte) 1, 4, 4, -4, null, "test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("chunkCount must be >= 0");

        assertThatThrownBy(() -> new EncryptedFileHeader((byte) 1, (byte) 1, 4, 4, 4, null, "test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fileNoncePrefix must be 8 bytes");

        assertThatNoException().isThrownBy(() -> new EncryptedFileHeader((byte) 1, (byte) 1, 4, 4, 4, "12345678".getBytes(StandardCharsets.UTF_8), "test"));
    }

    @Test
    public void testKeyTooLong() {
        assertThatThrownBy(() -> new EncryptedFileHeader((byte) 1, (byte) 1, 4, 4, 4, "12345678".getBytes(StandardCharsets.UTF_8),
                RandomStringUtils.secure().nextAlphanumeric(67_000)).serialize())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("keyId is too long");
    }

    @Test
    public void testParseFailures() {
        assertThatThrownBy(() -> EncryptedFileHeader.parse(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Not enough bytes to parse encrypted file header");

        assertThatThrownBy(() -> EncryptedFileHeader.parse("1234".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Not enough bytes to parse encrypted file header");

        assertThatThrownBy(() -> EncryptedFileHeader.parse(RandomStringUtils.secure().nextAlphanumeric(123)
                .getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid encrypted file header magic");

        assertThatThrownBy(() -> EncryptedFileHeader.parse("TFSE-----------------------------------"
                .getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Incomplete encrypted file header");
    }
}