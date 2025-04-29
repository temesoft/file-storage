package com.temesoft.fs.spring;

import com.temesoft.fs.FileStorageService;
import com.temesoft.fs.TestApp;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

public class FileStorageBeanRegistryConfigurationNoStoreTest extends TestApp {

    @Autowired
    private ApplicationContext context;

    @Test
    public void testDisabledFunctionality() {
        assertThat(context.getBeansOfType(FileStorageBeanRegistryConfiguration.class)).hasSize(1);
        // when no configuration is provided no service beans should exist
        assertThat(context.getBeansOfType(FileStorageService.class)).isEmpty();
        assertThat(context.getBeansOfType(FileStorageProperties.class)).isNotEmpty();
        final FileStorageProperties properties = context.getBean(FileStorageProperties.class);
        assertThat(properties.getInstances()).isEmpty();
    }
}