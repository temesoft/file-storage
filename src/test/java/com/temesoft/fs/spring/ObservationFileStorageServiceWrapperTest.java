package com.temesoft.fs.spring;

import com.github.ksuid.Ksuid;
import com.temesoft.fs.FileStorageId;
import com.temesoft.fs.FileStorageIdService;
import com.temesoft.fs.FileStorageService;
import com.temesoft.fs.TestBase;
import io.micrometer.observation.tck.TestObservationRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.TestPropertySource;

import java.io.ByteArrayInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.secure;
import static org.assertj.core.api.Assertions.assertThat;

@TestPropertySource(properties = {
        "app.file-storage.instances.widget-mem.type=InMemory",
        "app.file-storage.instances.widget-mem.bean-qualifier=service",
        "app.file-storage.instances.widget-mem.custom-wrappers=com.temesoft.fs.spring.ObservationFileStorageServiceWrapper",
        "app.file-storage.instances.widget-mem.entity-class=com.temesoft.fs.spring.ObservationFileStorageServiceWrapperTest$Widget",
        "app.file-storage.instances.widget-mem.id-service=com.temesoft.fs.spring.ObservationFileStorageServiceWrapperTest$StringIdService",
        "app.file-storage.instances.widget-mem.encryption.enabled=true",
        "app.file-storage.instances.widget-mem.encryption.algorithm=AES_GCM_CHUNKED",
        "app.file-storage.instances.widget-mem.encryption.chunk-size=128",
        "app.file-storage.instances.widget-mem.encryption.key-id=main",
        "app.file-storage.instances.widget-mem.encryption.base64-key=V3dYQjZ2cWJ5QjVyR0t3Y0tVS2N6N0pqZGRxNzR2cVg="
})
@Import(ObservationFileStorageServiceWrapperTest.TestConfig.class)
@EnableAutoConfiguration
class ObservationFileStorageServiceWrapperTest extends TestBase {

    private static final byte[] BYTE_CONTENT = secure().nextAlphanumeric(1024).getBytes(UTF_8);

    @Autowired
    private FileStorageService<String> service;
    @Autowired
    private TestObservationRegistry registry;

    @Test
    public void testObservation() {
        final String id = Ksuid.newKsuid().toString();
        assertThat(service).isNotNull();
        assertThat(service.getFileStorageIdService()).isInstanceOf(FileStorageIdService.class);
        assertThat(registry).isNotNull();

        service.create(id, BYTE_CONTENT);
        verifyObservation("create");
        service.exists(id);
        verifyObservation("exists");
        service.doesNotExist(id);
        verifyObservation("exists");
        service.getSize(id);
        verifyObservation("get_size");
        service.delete(id);
        verifyObservation("delete");
        service.create(id, BYTE_CONTENT);
        verifyObservation("create");
        service.delete(id);
        verifyObservation("delete");
        service.create(id, new ByteArrayInputStream(BYTE_CONTENT), BYTE_CONTENT.length);
        verifyObservation("create_stream");
        service.getBytes(id);
        verifyObservation("get_bytes");
        service.getBytes(id, 123, 456);
        verifyObservation("get_bytes_range");
        service.getInputStream(id);
        verifyObservation("get_input_stream");
        service.delete(id);
        verifyObservation("delete");
        service.deleteAll();
        verifyObservation("delete_all");
    }

    private void verifyObservation(final String operation) {
        assertThat(registry)
                .hasObservationWithNameEqualTo("file.storage")
                .that()
                .hasLowCardinalityKeyValue("operation", operation)
                .hasLowCardinalityKeyValue("storage.type", "in-memory-file-storage")
                .hasBeenStarted()
                .hasBeenStopped();
        registry.clear();
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

    @Configuration
    static class TestConfig {
        @Primary
        @Bean
        TestObservationRegistry testObservationRegistry() {
            return TestObservationRegistry.builder().build();
        }
    }
}