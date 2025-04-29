package com.temesoft.fs.spring;

import com.temesoft.fs.FileStorageId;
import com.temesoft.fs.FileStorageIdService;
import com.temesoft.fs.TestApp;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_SINGLETON;

@Import(TestApp.TestConfig.class)
@TestPropertySource(locations = "classpath:application-fs-test.properties")
@EnableAutoConfiguration
class FileStorageBeanRegistryConfigurationValidationTest extends TestApp {

    @Autowired
    private ApplicationContext context;
    @Autowired
    private Environment environment;

    @Test
    public void testPostProcessBeanDefinitionRegistry() {
        final FileStorageBeanRegistryConfiguration fileStorageRegistry = new FileStorageBeanRegistryConfiguration();
        final BeanDefinitionRegistry beanDefinitionRegistry = mock(BeanDefinitionRegistry.class);
        fileStorageRegistry.setEnvironment(environment);
        fileStorageRegistry.setApplicationContext(context);

        assertThatThrownBy(() -> {
            final FileStorageProperties props = new FileStorageProperties();
            final FileStorageProperties.FileStorageConfig config = new FileStorageProperties.FileStorageConfig();
            config.setBeanQualifier("test1");
            config.setEntityClass(String.class.getName());
            config.setIdService(StringIdService.class.getName());
            props.setInstances(Map.of("test1", config));
            fileStorageRegistry.postProcessInstancesToBeans(beanDefinitionRegistry, props);
        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing configuration for storage type: app.file-storage.instances.test1.type");

        assertThatThrownBy(() -> {
            final FileStorageProperties props = new FileStorageProperties();
            final FileStorageProperties.FileStorageConfig config = new FileStorageProperties.FileStorageConfig();
            config.setType(FileStorageProperties.FileStorageOption.InMemory);
            config.setEntityClass(String.class.getName());
            config.setIdService(StringIdService.class.getName());
            props.setInstances(Map.of("test1", config));
            fileStorageRegistry.postProcessInstancesToBeans(beanDefinitionRegistry, props);
        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing configuration for bean qualifier: app.file-storage.instances.test1.bean-qualifier");

        assertThatThrownBy(() -> {
            final FileStorageProperties props = new FileStorageProperties();
            final FileStorageProperties.FileStorageConfig config = new FileStorageProperties.FileStorageConfig();
            config.setType(FileStorageProperties.FileStorageOption.InMemory);
            config.setBeanQualifier("test1");
            config.setIdService(StringIdService.class.getName());
            props.setInstances(Map.of("test1", config));
            fileStorageRegistry.postProcessInstancesToBeans(beanDefinitionRegistry, props);
        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing configuration for entity class: app.file-storage.instances.test1.entity-class");

        assertThatThrownBy(() -> {
            final FileStorageProperties props = new FileStorageProperties();
            final FileStorageProperties.FileStorageConfig config = new FileStorageProperties.FileStorageConfig();
            config.setType(FileStorageProperties.FileStorageOption.InMemory);
            config.setBeanQualifier("test1");
            config.setEntityClass(String.class.getName());
            props.setInstances(Map.of("test1", config));
            fileStorageRegistry.postProcessInstancesToBeans(beanDefinitionRegistry, props);
        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing configuration for id service: app.file-storage.instances.test1.id-service");

        assertThatThrownBy(() -> {
            final FileStorageProperties props = new FileStorageProperties();
            final FileStorageProperties.FileStorageConfig config = new FileStorageProperties.FileStorageConfig();
            config.setType(FileStorageProperties.FileStorageOption.InMemory);
            config.setBeanQualifier("test1");
            config.setEntityClass("com.some.where.BadEntityClass");
            config.setIdService(StringIdService.class.getName());
            props.setInstances(Map.of("test1", config));
            fileStorageRegistry.postProcessInstancesToBeans(beanDefinitionRegistry, props);
        })
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(ClassNotFoundException.class)
                .hasMessage("java.lang.ClassNotFoundException: com.some.where.BadEntityClass");

        assertThatThrownBy(() -> {
            final FileStorageProperties props = new FileStorageProperties();
            final FileStorageProperties.FileStorageConfig config = new FileStorageProperties.FileStorageConfig();
            config.setType(FileStorageProperties.FileStorageOption.InMemory);
            config.setBeanQualifier("test1");
            config.setEntityClass(String.class.getName());
            config.setIdService("com.some.where.BadIdServiceClass");
            props.setInstances(Map.of("test1", config));
            fileStorageRegistry.postProcessInstancesToBeans(beanDefinitionRegistry, props);
        })
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(ClassNotFoundException.class)
                .hasMessage("java.lang.ClassNotFoundException: com.some.where.BadIdServiceClass");
    }

    @Test
    public void testCreateFileStorageServiceSystem() {
        final FileStorageBeanRegistryConfiguration fileStorageRegistry = new FileStorageBeanRegistryConfiguration();
        final BeanDefinitionRegistry beanDefinitionRegistry = mock(BeanDefinitionRegistry.class);
        fileStorageRegistry.setEnvironment(environment);
        fileStorageRegistry.setApplicationContext(context);

        final FileStorageProperties.FileStorageConfig config = new FileStorageProperties.FileStorageConfig();
        config.setType(FileStorageProperties.FileStorageOption.InMemory);
        config.setBeanQualifier("test1");
        config.setEntityClass(String.class.getName());
        config.setIdService(StringIdService.class.getName());

        final GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
        beanDefinition.setScope(SCOPE_SINGLETON);

        assertThatThrownBy(() -> {
            fileStorageRegistry.createFileStorageServiceSystem(beanDefinition, StringIdService.class, config, "test1");
        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing configuration for system root location: app.file-storage.instances.test1.system.root-location");
    }

    @Test
    public void testCreateFileStorageServiceSftp() {
        final FileStorageBeanRegistryConfiguration fileStorageRegistry = new FileStorageBeanRegistryConfiguration();
        fileStorageRegistry.setEnvironment(environment);
        fileStorageRegistry.setApplicationContext(context);

        final FileStorageProperties.FileStorageConfig config = new FileStorageProperties.FileStorageConfig();
        config.setType(FileStorageProperties.FileStorageOption.Sftp);
        config.setBeanQualifier("test1");
        config.setEntityClass(String.class.getName());
        config.setIdService(StringIdService.class.getName());

        final GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
        beanDefinition.setScope(SCOPE_SINGLETON);

        assertThatThrownBy(() -> {
            fileStorageRegistry.createFileStorageServiceSftp(beanDefinition, StringIdService.class, config, "test1");
        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing configuration for sftp remote host: app.file-storage.instances.test1.sftp.remote-host");

        config.getSftp().setRemoteHost("127.0.0.1");

        assertThatThrownBy(() -> {
            fileStorageRegistry.createFileStorageServiceSftp(beanDefinition, StringIdService.class, config, "test1");
        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing configuration for sftp remote port: app.file-storage.instances.test1.sftp.remote-port");

        config.getSftp().setRemotePort(12345);

        assertThatThrownBy(() -> {
            fileStorageRegistry.createFileStorageServiceSftp(beanDefinition, StringIdService.class, config, "test1");
        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing configuration for sftp username: app.file-storage.instances.test1.sftp.username");

        config.getSftp().setUsername("tester");

        assertThatThrownBy(() -> {
            fileStorageRegistry.createFileStorageServiceSftp(beanDefinition, StringIdService.class, config, "test1");
        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing configuration for sftp password: app.file-storage.instances.test1.sftp.password");

        config.getSftp().setPassword("tester");

        assertThatThrownBy(() -> {
            fileStorageRegistry.createFileStorageServiceSftp(beanDefinition, StringIdService.class, config, "test1");
        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing configuration for sftp root directory: app.file-storage.instances.test1.sftp.root-directory");

        config.getSftp().setRootDirectory("./test");

        assertThatNoException().isThrownBy(() -> fileStorageRegistry
                .createFileStorageServiceSftp(beanDefinition, StringIdService.class, config, "test1"));
    }

    @Test
    public void testCreateFileStorageServiceS3() {
        final FileStorageBeanRegistryConfiguration fileStorageRegistry = new FileStorageBeanRegistryConfiguration();
        fileStorageRegistry.setEnvironment(environment);
        fileStorageRegistry.setApplicationContext(context);

        final FileStorageProperties.FileStorageConfig config = new FileStorageProperties.FileStorageConfig();
        config.setType(FileStorageProperties.FileStorageOption.S3);
        config.setBeanQualifier("test1");
        config.setEntityClass(String.class.getName());
        config.setIdService(StringIdService.class.getName());

        final GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
        beanDefinition.setScope(SCOPE_SINGLETON);

        assertThatThrownBy(() -> {
            fileStorageRegistry.createFileStorageServiceS3(beanDefinition, StringIdService.class, config, "test1");
        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing configuration for S3 bucket name: app.file-storage.instances.test1.s3.bucket-name");

        config.getS3().setBucketName("test");

        assertThatNoException().isThrownBy(() -> fileStorageRegistry
                .createFileStorageServiceS3(beanDefinition, StringIdService.class, config, "test1"));
    }

    @Test
    public void testCreateFileStorageServiceGcs() {
        final FileStorageBeanRegistryConfiguration fileStorageRegistry = new FileStorageBeanRegistryConfiguration();
        fileStorageRegistry.setEnvironment(environment);
        fileStorageRegistry.setApplicationContext(context);

        final FileStorageProperties.FileStorageConfig config = new FileStorageProperties.FileStorageConfig();
        config.setType(FileStorageProperties.FileStorageOption.GCS);
        config.setBeanQualifier("test1");
        config.setEntityClass(String.class.getName());
        config.setIdService(StringIdService.class.getName());

        final GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
        beanDefinition.setScope(SCOPE_SINGLETON);

        assertThatThrownBy(() -> {
            fileStorageRegistry.createFileStorageServiceGcs(beanDefinition, StringIdService.class, config, "test1");
        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Missing configuration for GCS bucket name: app.file-storage.instances.test1.gcs.bucket-name");

        config.getGcs().setBucketName("test");

        assertThatNoException().isThrownBy(() -> fileStorageRegistry
                .createFileStorageServiceGcs(beanDefinition, StringIdService.class, config, "test1"));
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
}