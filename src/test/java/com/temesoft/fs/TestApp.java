package com.temesoft.fs;

import com.azure.storage.blob.BlobContainerClient;
import com.google.cloud.storage.Storage;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.http.client.HttpClientAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;
import software.amazon.awssdk.services.s3.S3Client;

import static org.mockito.Mockito.mock;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@SpringBootApplication(exclude = {HttpClientAutoConfiguration.class})
@TestPropertySource(properties = {"spring.main.banner-mode=off"})
public class TestApp {

    @Configuration
    public static class TestConfig {
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

}
