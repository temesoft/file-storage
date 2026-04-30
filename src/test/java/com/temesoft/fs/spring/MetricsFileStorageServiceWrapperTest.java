package com.temesoft.fs.spring;

import com.github.ksuid.Ksuid;
import com.temesoft.fs.FileStorageId;
import com.temesoft.fs.FileStorageIdService;
import com.temesoft.fs.FileStorageService;
import com.temesoft.fs.TestBase;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.test.context.TestPropertySource;

import java.io.ByteArrayInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.secure;
import static org.assertj.core.api.Assertions.assertThat;

@TestPropertySource(properties = {
        "app.file-storage.instances.widget-mem.type=InMemory",
        "app.file-storage.instances.widget-mem.bean-qualifier=service",
        "app.file-storage.instances.widget-mem.custom-wrappers=com.temesoft.fs.spring.MetricsFileStorageServiceWrapper",
        "app.file-storage.instances.widget-mem.entity-class=com.temesoft.fs.spring.MetricsFileStorageServiceWrapperTest$Widget",
        "app.file-storage.instances.widget-mem.id-service=com.temesoft.fs.spring.MetricsFileStorageServiceWrapperTest$StringIdService",
        "app.file-storage.instances.widget-mem.encryption.enabled=true",
        "app.file-storage.instances.widget-mem.encryption.algorithm=AES_GCM_CHUNKED",
        "app.file-storage.instances.widget-mem.encryption.chunk-size=128",
        "app.file-storage.instances.widget-mem.encryption.key-id=main",
        "app.file-storage.instances.widget-mem.encryption.base64-key=V3dYQjZ2cWJ5QjVyR0t3Y0tVS2N6N0pqZGRxNzR2cVg="
})
@EnableAutoConfiguration
class MetricsFileStorageServiceWrapperTest extends TestBase {

    private static final byte[] BYTE_CONTENT = secure().nextAlphanumeric(1024).getBytes(UTF_8);

    @Autowired
    private FileStorageService<String> service;
    @Autowired
    private MeterRegistry registry;

    @Test
    public void testMetrics() {
        final String id = Ksuid.newKsuid().toString();
        assertThat(service).isNotNull();
        assertThat(service.getFileStorageIdService()).isInstanceOf(FileStorageIdService.class);
        assertThat(registry).isNotNull();

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

        verifyTimer("create_bytes", 2);
        verifyTimer("delete", 3);
        verifyTimer("delete_all", 1);
        verifyTimer("exists", 2);
        verifyTimer("get_input_stream", 1);
        verifyTimer("create_stream", 1);
        verifyTimer("get_size", 1);
        verifyTimer("get_bytes_range", 1);
        verifyTimer("get_bytes", 1);
        verifyTimer("create_bytes", 2);
    }

    private void verifyTimer(final String operation, final long count) {
        final Timer timer = registry.find("file.storage.in-memory-file-storage")
                .tag("operation", operation)
                .timer();
        assertThat(timer).isNotNull();
        assertThat(timer.count()).isEqualTo(count);
    }

    static class StringIdService implements FileStorageIdService<String> {
        @Override
        public FileStorageId<String> fromId(final String value) {
            return new FileStorageId<>(value) {
                @Override
                public String generatePath() {
                    return value();
                }
            };
        }
    }

    public static class Widget {
        private String id;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }
    }
}