package com.temesoft.fs.spring;

import com.github.ksuid.Ksuid;
import com.temesoft.fs.FileStorageId;
import com.temesoft.fs.FileStorageIdService;
import com.temesoft.fs.FileStorageService;
import com.temesoft.fs.FileStorageServiceWrapper;
import com.temesoft.fs.TestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.test.context.TestPropertySource;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.secure;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

@TestPropertySource(properties = {
        "app.file-storage.instances.widget-mem.type=InMemory",
        "app.file-storage.instances.widget-mem.bean-qualifier=service",
        "app.file-storage.instances.widget-mem.custom-wrappers=" +
                "com.temesoft.fs.spring.MetricsFileStorageServiceWrapper," +
                "com.temesoft.fs.spring.ObservationFileStorageServiceWrapper," +
                "com.temesoft.fs.spring.MultipleWrappersTest$CustomWrapper",
        "app.file-storage.instances.widget-mem.entity-class=com.temesoft.fs.spring.MultipleWrappersTest$Widget",
        "app.file-storage.instances.widget-mem.id-service=com.temesoft.fs.spring.MultipleWrappersTest$StringIdService",
        "app.file-storage.instances.widget-mem.encryption.enabled=true",
        "app.file-storage.instances.widget-mem.encryption.algorithm=AES_GCM_CHUNKED",
        "app.file-storage.instances.widget-mem.encryption.chunk-size=128",
        "app.file-storage.instances.widget-mem.encryption.key-id=main",
        "app.file-storage.instances.widget-mem.encryption.base64-key=V3dYQjZ2cWJ5QjVyR0t3Y0tVS2N6N0pqZGRxNzR2cVg="
})
@EnableAutoConfiguration
public class MultipleWrappersTest extends TestBase {

    private static final byte[] BYTE_CONTENT = secure().nextAlphanumeric(1024).getBytes(UTF_8);

    @Autowired
    private FileStorageService<String> service;
    @Autowired
    private FileStorageBeanRegistryConfiguration.FileStorageEndpoint fileStorageEndpoint;

    @Test
    public void testMetrics() {
        final String id = Ksuid.newKsuid().toString();
        assertThat(service).isNotNull();
        assertThat(service.getFileStorageIdService()).isInstanceOf(FileStorageIdService.class);

        assertThatNoException().isThrownBy(() -> {
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
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testActuatorEndpoint() {
        final Map<String, Object> endpointModel = fileStorageEndpoint.viewRegisteredFileStorages();
        assertThat(endpointModel)
                .hasSize(1)
                .containsKey("service");
        final Map<String, Object> serviceModel = (Map<String, Object>) endpointModel.get("service");
        assertThat(serviceModel)
                .hasSize(4)
                .containsKeys("description", "idService", "storageService", "wrapperLayers");
        assertThat(serviceModel.get("description"))
                .isEqualTo("Observation(Metrics(Logging(Encrypted(In memory file storage))))");
        assertThat(serviceModel.get("idService"))
                .isEqualTo("com.temesoft.fs.spring.MultipleWrappersTest$StringIdService");
        assertThat(serviceModel.get("storageService"))
                .isEqualTo("com.temesoft.fs.InMemoryFileStorageServiceImpl");
        final List<String> layers = (List<String>) serviceModel.get("wrapperLayers");
        assertThat(layers)
                .hasSize(5)
                .containsExactly(
                        "com.temesoft.fs.spring.MultipleWrappersTest$CustomWrapper",
                        "com.temesoft.fs.spring.ObservationFileStorageServiceWrapper",
                        "com.temesoft.fs.spring.MetricsFileStorageServiceWrapper",
                        "com.temesoft.fs.LoggingFileStorageServiceWrapper",
                        "com.temesoft.fs.EncryptingFileStorageServiceWrapper"
                );
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

    static class Widget {
        private String id;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }
    }

    public static class CustomWrapper implements FileStorageServiceWrapper<String> {

        private final FileStorageService<String> delegate;

        public CustomWrapper(final FileStorageService<String> delegate) {
            this.delegate = delegate;
        }

        @Override
        public FileStorageService<String> getService() {
            return delegate;
        }
    }
}