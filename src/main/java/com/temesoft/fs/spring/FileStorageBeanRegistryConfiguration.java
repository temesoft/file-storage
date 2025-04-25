package com.temesoft.fs.spring;

import com.azure.storage.blob.BlobContainerClient;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Maps;
import com.temesoft.fs.AzureFileStorageServiceImpl;
import com.temesoft.fs.FileStorageIdService;
import com.temesoft.fs.FileStorageService;
import com.temesoft.fs.GcsFileStorageServiceImpl;
import com.temesoft.fs.HdfsFileStorageServiceImpl;
import com.temesoft.fs.InMemoryFileStorageServiceImpl;
import com.temesoft.fs.S3FileStorageServiceImpl;
import com.temesoft.fs.SftpFileStorageServiceImpl;
import com.temesoft.fs.SystemFileStorageServiceImpl;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Map;

import static org.springframework.util.ObjectUtils.isEmpty;

/**
 * Configuration class setting up a bean registry approach to creating one or more beans {@link FileStorageService}
 * specified by configuration defined in {@link FileStorageProperties}.
 */
@Configuration
@Order(Ordered.HIGHEST_PRECEDENCE)
public class FileStorageBeanRegistryConfiguration implements BeanDefinitionRegistryPostProcessor, EnvironmentAware, ApplicationContextAware {

    public static final String ENDPOINT_ID = "file-storage";

    private static final Logger LOGGER = LoggerFactory.getLogger(FileStorageBeanRegistryConfiguration.class);
    private static final String USAGE_MESSAGE =
            "For file storage configuration '{}.instances.{}.*' an implementation '{}' is being configured " +
            "for entity '{}' and id service '{}' as bean '{}'";
    private static final String GENERATING_MESSAGE = "Generating {} {} bean(s)";
    private static final String PROPERTY_NAME_PORTION = ".instances.%s.";

    private static final Map<String, FileStorageService<?>> BEAN_MAP = Maps.newConcurrentMap();

    private ApplicationContext context;
    private Environment environment;

    @Override
    public void setEnvironment(final Environment env) {
        this.environment = env;
    }

    @Override
    public void setApplicationContext(final ApplicationContext ctx) throws BeansException {
        this.context = ctx;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(final BeanDefinitionRegistry registry) throws BeansException {
        final FileStorageProperties fileStorageProperties = Binder.get(environment)
                .bind(FileStorageProperties.PREFIX, Bindable.of(FileStorageProperties.class))
                .orElse(new FileStorageProperties());
        if (isEmpty(fileStorageProperties.getInstances())) {
            return;
        }

        BEAN_MAP.clear();
        LOGGER.info(
                GENERATING_MESSAGE,
                fileStorageProperties.getInstances().size(),
                FileStorageService.class.getSimpleName()
        );
        fileStorageProperties.getInstances().forEach((key, config) -> {
            final GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
            beanDefinition.setScope(BeanDefinition.SCOPE_SINGLETON);
            try {
                if (config.getType() == null) {
                    throw new IllegalStateException("Missing configuration for storage type: " +
                                                    FileStorageProperties.PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "type");
                } else if (isEmpty(config.getEntityClass())) {
                    throw new IllegalStateException("Missing configuration for entity class: " +
                                                    FileStorageProperties.PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "entity-class");
                } else if (isEmpty(config.getIdService())) {
                    throw new IllegalStateException("Missing configuration for id service: " +
                                                    FileStorageProperties.PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "id-service");
                } else if (isEmpty(config.getBeanQualifier())) {
                    throw new IllegalStateException("Missing configuration for bean qualifier: " +
                                                    FileStorageProperties.PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "bean-qualifier");
                }
                final Class<?> idService = Class.forName(config.getIdService());
                final Class<?> entityClass = Class.forName(config.getEntityClass());

                if (config.getType() == FileStorageProperties.FileStorageOption.InMemory) {
                    createFileStorageServiceInMemory(beanDefinition, idService);
                } else if (config.getType() == FileStorageProperties.FileStorageOption.System) {
                    createFileStorageServiceSystem(beanDefinition, idService, config, key);
                } else if (config.getType() == FileStorageProperties.FileStorageOption.Sftp) {
                    createFileStorageServiceSftp(beanDefinition, idService, config, key);
                } else if (config.getType() == FileStorageProperties.FileStorageOption.S3) {
                    createFileStorageServiceS3(beanDefinition, idService, config, key);
                } else if (config.getType() == FileStorageProperties.FileStorageOption.GCS) {
                    createFileStorageServiceGcs(beanDefinition, idService, config, key);
                } else if (config.getType() == FileStorageProperties.FileStorageOption.Azure) {
                    createFileStorageServiceAzure(beanDefinition, idService);
                } else if (config.getType() == FileStorageProperties.FileStorageOption.HDFS) {
                    createFileStorageServiceHdfs(beanDefinition, idService);
                } else {
                    throw new IllegalStateException("Not configured to create storage of this type: " + config.getType());
                }

                LOGGER.debug(USAGE_MESSAGE,
                        FileStorageProperties.PREFIX,
                        key,
                        beanDefinition.getBeanClass().getSimpleName(),
                        entityClass.getSimpleName(),
                        idService.getSimpleName(),
                        config.getBeanQualifier()
                );

                if (beanDefinition.getInstanceSupplier() != null
                    && beanDefinition.getInstanceSupplier().get() != null) {
                    BEAN_MAP.put(config.getBeanQualifier(), (FileStorageService<?>) beanDefinition.getInstanceSupplier().get());
                }

                beanDefinition.addQualifier(new AutowireCandidateQualifier(config.getBeanQualifier()));
                registry.registerBeanDefinition(config.getBeanQualifier(), beanDefinition);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                     NoSuchMethodException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Registers custom actuator endpoint describing file storage beans registered.
     * Can be disabled using configuration property: `app.file-storage.endpoint-enabled=false`
     */
    @Component
    @ConditionalOnProperty(value = "${" + FileStorageProperties.PREFIX + ".endpoint-enabled:true}", matchIfMissing = true, havingValue = "true")
    @Endpoint(id = FileStorageBeanRegistryConfiguration.ENDPOINT_ID)
    static class FileStorageEndpointConfiguration {
        /**
         * Endpoint read operation, when enabled - accessible via /actuator/file-storage
         */
        @ReadOperation
        public Map<String, FileStorageService<?>> viewFileStorages() {
            return BEAN_MAP;
        }
    }

    /**
     * Generates file storage service for InMemory based on provided config and idService
     */
    private void createFileStorageServiceInMemory(final GenericBeanDefinition beanDefinition,
                                                  final Class<?> idService) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        beanDefinition.setBeanClass(InMemoryFileStorageServiceImpl.class);
        final FileStorageService<?> srv = new InMemoryFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance()
        );
        beanDefinition.setInstanceSupplier(() -> srv);
    }

    /**
     * Generates file storage service for System based on provided config and idService
     */
    private void createFileStorageServiceSystem(final GenericBeanDefinition beanDefinition,
                                                final Class<?> idService,
                                                final FileStorageProperties.FileStorageConfig config,
                                                final String key) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (isEmpty(config.getSystem().getRootLocation())) {
            throw new IllegalStateException("Missing configuration for system root location: " +
                                            FileStorageProperties.PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "system.root-location");
        }
        beanDefinition.setBeanClass(SystemFileStorageServiceImpl.class);
        final FileStorageService<?> srv = new SystemFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                Path.of(config.getSystem().getRootLocation())
        );
        beanDefinition.setInstanceSupplier(() -> srv);
    }

    /**
     * Generates file storage service for SFTP based on provided config and idService
     */
    private void createFileStorageServiceSftp(final GenericBeanDefinition beanDefinition,
                                              final Class<?> idService,
                                              final FileStorageProperties.FileStorageConfig config,
                                              final String key) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (isEmpty(config.getSftp().getRemoteHost())) {
            throw new IllegalStateException("Missing configuration for sftp remote host: " +
                                            FileStorageProperties.PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "sftp.remote-host");
        } else if (isEmpty(config.getSftp().getRemotePort())) {
            throw new IllegalStateException("Missing configuration for sftp remote port: " +
                                            FileStorageProperties.PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "sftp.remote-port");
        } else if (isEmpty(config.getSftp().getUsername())) {
            throw new IllegalStateException("Missing configuration for sftp username: " +
                                            FileStorageProperties.PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "sftp.username");
        } else if (isEmpty(config.getSftp().getPassword())) {
            throw new IllegalStateException("Missing configuration for sftp password: " +
                                            FileStorageProperties.PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "sftp.password");
        } else if (isEmpty(config.getSftp().getRootDirectory())) {
            throw new IllegalStateException("Missing configuration for sftp root directory: " +
                                            FileStorageProperties.PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "sftp.root-directory");
        }
        beanDefinition.setBeanClass(SftpFileStorageServiceImpl.class);
        final FileStorageService<?> srv = new SftpFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                config.getSftp().getRemoteHost(),
                config.getSftp().getRemotePort(),
                config.getSftp().getUsername(),
                config.getSftp().getPassword(),
                config.getSftp().getRootDirectory(),
                config.getSftp().getConfigProperties()
        );
        beanDefinition.setInstanceSupplier(() -> srv);
    }

    /**
     * Generates file storage service for S3 based on provided config and idService
     */
    private void createFileStorageServiceS3(final GenericBeanDefinition beanDefinition,
                                            final Class<?> idService,
                                            final FileStorageProperties.FileStorageConfig config,
                                            final String key) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (isEmpty(config.getS3().getBucketName())) {
            throw new IllegalStateException("Missing configuration for S3 bucket name: " +
                                            FileStorageProperties.PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "s3.bucket-name");
        }
        beanDefinition.setBeanClass(S3FileStorageServiceImpl.class);
        final FileStorageService<?> srv = new S3FileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                context.getBean(S3Client.class),
                config.getS3().getBucketName()
        );
        beanDefinition.setInstanceSupplier(() -> srv);
    }

    /**
     * Generates file storage service for GCS based on provided config and idService
     */
    private void createFileStorageServiceGcs(final GenericBeanDefinition beanDefinition,
                                             final Class<?> idService,
                                             final FileStorageProperties.FileStorageConfig config,
                                             final String key) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (isEmpty(config.getGcs().getBucketName())) {
            throw new IllegalStateException("Missing configuration for GCS bucket name: " +
                                            FileStorageProperties.PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "gcs.bucket-name");
        }
        beanDefinition.setBeanClass(GcsFileStorageServiceImpl.class);
        final FileStorageService<?> srv = new GcsFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                context.getBean(Storage.class),
                config.getGcs().getBucketName()
        );
        beanDefinition.setInstanceSupplier(() -> srv);
    }

    /**
     * Generates file storage service for Azure based on provided config and idService
     */
    private void createFileStorageServiceAzure(final GenericBeanDefinition beanDefinition,
                                               final Class<?> idService) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        beanDefinition.setBeanClass(AzureFileStorageServiceImpl.class);
        final FileStorageService<?> srv = new AzureFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                context.getBean(BlobContainerClient.class)
        );
        beanDefinition.setInstanceSupplier(() -> srv);
    }

    /**
     * Generates file storage service for HDFS based on provided config and idService
     */
    private void createFileStorageServiceHdfs(final GenericBeanDefinition beanDefinition,
                                              final Class<?> idService) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        beanDefinition.setBeanClass(HdfsFileStorageServiceImpl.class);
        final FileStorageService<?> srv = new HdfsFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                context.getBean(FileSystem.class)
        );
        beanDefinition.setInstanceSupplier(() -> srv);
    }
}
