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
import com.temesoft.fs.S3FileStorageServiceImpl;
import com.temesoft.fs.SftpFileStorageServiceImpl;
import com.temesoft.fs.SystemFileStorageServiceImpl;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.http.client.HttpClientAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.TestPropertySource;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@SpringBootApplication(exclude = {HttpClientAutoConfiguration.class})
@PropertySource("application-fs-test.properties")
@TestPropertySource(properties = "spring.main.banner-mode=off")
@Import(FileStorageBeanFactoryConfigurationTest.TestClass.class)
class FileStorageBeanFactoryConfigurationTest {

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

        assertThat(widgetFileStorage)
                .isInstanceOf(InMemoryFileStorageServiceImpl.class);

        assertThat(context.getBean("widgetFileStorage", FileStorageService.class))
                .isEqualTo(widgetFileStorage)
                .isInstanceOf(InMemoryFileStorageServiceImpl.class);

        assertThat(context.getBean("trinketFileStorage", FileStorageService.class))
                .isInstanceOf(InMemoryFileStorageServiceImpl.class);

        assertThat(context.getBean("trinketSysFileStorage", FileStorageService.class))
                .isInstanceOf(SystemFileStorageServiceImpl.class);

        assertThat(context.getBean("trinketSftpFileStorage", FileStorageService.class))
                .isInstanceOf(SftpFileStorageServiceImpl.class);

        assertThat(context.getBean("trinketS3FileStorage", FileStorageService.class))
                .isInstanceOf(S3FileStorageServiceImpl.class);

        assertThat(context.getBean("trinketGcsFileStorage", FileStorageService.class))
                .isInstanceOf(GcsFileStorageServiceImpl.class);

        assertThat(context.getBean("trinketAzureFileStorage", FileStorageService.class))
                .isInstanceOf(AzureFileStorageServiceImpl.class);

        assertThat(context.getBean("trinketHdfsFileStorage", FileStorageService.class))
                .isInstanceOf(HdfsFileStorageServiceImpl.class);
    }

    @Configuration
    static class TestClass {
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