package com.temesoft.fs.spring;

import com.azure.storage.blob.BlobContainerClient;
import com.google.cloud.storage.Storage;
import com.temesoft.fs.AzureFileStorageServiceImpl;
import com.temesoft.fs.FileStorageId;
import com.temesoft.fs.FileStorageIdService;
import com.temesoft.fs.FileStorageService;
import com.temesoft.fs.GcsFileStorageServiceImpl;
import com.temesoft.fs.HdfsFileStorageServiceImpl;
import com.temesoft.fs.InMemoryFileStorageServiceImpl;
import com.temesoft.fs.LoggingFileStorageServiceWrapper;
import com.temesoft.fs.S3FileStorageServiceImpl;
import com.temesoft.fs.SftpFileStorageServiceImpl;
import com.temesoft.fs.SystemFileStorageServiceImpl;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@TestPropertySource(locations = "classpath:application-fs-test.properties")
@Import(FileStorageBeanRegistryConfigurationTest.TestConfig.class)
@EnableAutoConfiguration
class FileStorageBeanRegistryConfigurationTest extends TestApp {

    @Autowired
    private List<FileStorageService<?>> fileStorageServiceList;
    @Autowired
    private ApplicationContext context;
    @Autowired
    @Qualifier("widgetFileStorage")
    private FileStorageService<Widget> widgetFileStorage;

    @Test
    public void testBasicFunctionality() {
        final String text = RandomStringUtils.secureStrong().nextAlphanumeric(20);
        final Widget widget = new Widget();
        widget.setId(UUID.randomUUID().toString());
        widgetFileStorage.create(widget, text.getBytes(UTF_8));
        final byte[] savedData = widgetFileStorage.getBytes(widget);
        assertThat(savedData).isNotEmpty().hasSize(20);
        assertThat(new String(savedData, UTF_8)).isEqualTo(text);
        widgetFileStorage.delete(widget);
        assertThat(widgetFileStorage.exists(widget)).isFalse();
    }

    @Test
    public void testSpringBeansByName() {
        assertThat(fileStorageServiceList)
                .isNotEmpty().hasSize(8);

        assertThat(widgetFileStorage).isInstanceOf(LoggingFileStorageServiceWrapper.class);
        assertThat(((LoggingFileStorageServiceWrapper<?>)widgetFileStorage).getService())
                .isInstanceOf(InMemoryFileStorageServiceImpl.class);

        assertThat(context.getBean("widgetFileStorage", LoggingFileStorageServiceWrapper.class).getService())
                .isInstanceOf(InMemoryFileStorageServiceImpl.class);

        assertThat(context.getBean("trinketFileStorage", LoggingFileStorageServiceWrapper.class).getService())
                .isInstanceOf(InMemoryFileStorageServiceImpl.class);

        assertThat(context.getBean("trinketSysFileStorage", LoggingFileStorageServiceWrapper.class).getService())
                .isInstanceOf(SystemFileStorageServiceImpl.class);

        assertThat(context.getBean("trinketSftpFileStorage", LoggingFileStorageServiceWrapper.class).getService())
                .isInstanceOf(SftpFileStorageServiceImpl.class);

        assertThat(context.getBean("trinketS3FileStorage", LoggingFileStorageServiceWrapper.class).getService())
                .isInstanceOf(S3FileStorageServiceImpl.class);

        assertThat(context.getBean("trinketGcsFileStorage", LoggingFileStorageServiceWrapper.class).getService())
                .isInstanceOf(GcsFileStorageServiceImpl.class);

        assertThat(context.getBean("trinketAzureFileStorage", LoggingFileStorageServiceWrapper.class).getService())
                .isInstanceOf(AzureFileStorageServiceImpl.class);

        assertThat(context.getBean("trinketHdfsFileStorage", LoggingFileStorageServiceWrapper.class).getService())
                .isInstanceOf(HdfsFileStorageServiceImpl.class);
    }

    @Configuration
    static class TestConfig {
        @Bean
        S3Client mockS3Client() {
            return mock(S3Client.class);
        }

        @Bean
        Storage mockStorage() {
            return mock(Storage.class);
        }

        @Bean
        BlobContainerClient mockBlobContainerClient() {
            return mock(BlobContainerClient.class);
        }

        @Bean
        FileSystem mockFileSystem() {
            return mock(FileSystem.class);
        }
    }

    public static class WidgetFileStorageIdService implements FileStorageIdService<Widget> {
        @Override
        public FileStorageId<Widget> fromId(final Widget e) {
            return new FileStorageId<>(e) {
                @Override
                public String generatePath() {
                    final String idString = value().getId();
                    return idString.charAt(0)
                           + SEPARATOR + idString.charAt(1)
                           + SEPARATOR + idString.charAt(2)
                           + SEPARATOR + idString.charAt(3)
                           + SEPARATOR + idString.substring(4)
                           + ".txt";
                }
            };
        }
    }

    public static class TrinketFileStorageIdService implements FileStorageIdService<Trinket> {
        @Override
        public FileStorageId<Trinket> fromId(final Trinket e) {
            return new FileStorageId<>(e) {
                @Override
                public String generatePath() {
                    final String idString = value().getId();
                    return idString.charAt(0)
                           + SEPARATOR + idString.charAt(1)
                           + SEPARATOR + idString.charAt(2)
                           + SEPARATOR + idString.charAt(3)
                           + SEPARATOR + idString.substring(4)
                           + ".txt";
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

    public static class Trinket {
        String id;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }
    }
}