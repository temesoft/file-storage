package com.temesoft.fs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.secure;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(OutputCaptureExtension.class)
@EnableAutoConfiguration
@TestPropertySource(properties = {
        "logging.level.com.temesoft.fs.InMemoryFileStorageServiceImpl=DEBUG",
        "logging.level.com.temesoft.fs.LoggingFileStorageServiceWrapperTest=DEBUG"
})
@DirtiesContext
class LoggingFileStorageServiceWrapperTest extends TestApp {
    final Logger logger = LoggerFactory.getLogger(LoggingFileStorageServiceWrapperTest.class);
    private static final byte[] BYTE_CONTENT = secure().nextAlphanumeric(1024).getBytes(UTF_8);

    @Test
    public void testLogging(final CapturedOutput output) {
        final UUID id = UUID.randomUUID();
        logger.debug("Verifying log entry: {}", id);
        final FileStorageService<UUID> service = new LoggingFileStorageServiceWrapper<>(
                new InMemoryFileStorageServiceImpl<>(value -> new FileStorageId<>(value) {
                    @Override
                    public String generatePath() {
                        return value().toString();
                    }
                })
        );

        service.create(id, BYTE_CONTENT);
        assertThat(service.getBytes(id)).hasSize(1024);
        service.deleteAll();

        assertThat(output)
                .contains("Verifying log entry: %s".formatted(id))
                .contains("create('%s', 1024 bytes)".formatted(id))
                .contains("getBytes('%s')".formatted(id))
                .contains("deleteAll()");
    }
}