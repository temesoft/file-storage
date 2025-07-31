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

import java.io.ByteArrayInputStream;
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
class LoggingFileStorageServiceWrapperTest extends TestBase {
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

        // following is just to create the log entries, no specific business logic
        service.create(id, BYTE_CONTENT);
        service.exists(id);
        service.doesNotExist(id);
        service.getSize(id);
        service.delete(id);
        service.create(id, BYTE_CONTENT);
        service.delete(id);
        service.create(id, new ByteArrayInputStream(BYTE_CONTENT), BYTE_CONTENT.length);
        service.getBytes(id);
        service.getBytes(id, 123, 456);
        service.getInputStream(id);
        service.delete(id);
        service.deleteAll();

        assertThat(output)
                .contains("Verifying log entry: %s".formatted(id))
                .contains("create('%s', 1024 bytes)".formatted(id))
                .contains("exists('%s')".formatted(id))
                .contains("doesNotExist('%s')".formatted(id))
                .contains("getSize('%s')".formatted(id))
                .contains("create('%s', java.io.ByteArrayInputStream".formatted(id))
                .contains("getBytes('%s')".formatted(id))
                .contains("getBytes('%s')".formatted(id))
                .contains("getBytes('%s', 123, 456)".formatted(id))
                .contains("getInputStream('%s')".formatted(id))
                .contains("delete('%s')".formatted(id))
                .contains("deleteAll()");
    }
}