package com.temesoft.fs.spring;

import com.azure.storage.blob.BlobContainerClient;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.temesoft.fs.AzureFileStorageServiceImpl;
import com.temesoft.fs.FileStorageIdService;
import com.temesoft.fs.FileStorageService;
import com.temesoft.fs.GcsFileStorageServiceImpl;
import com.temesoft.fs.HdfsFileStorageServiceImpl;
import com.temesoft.fs.InMemoryFileStorageServiceImpl;
import com.temesoft.fs.LoggingFileStorageServiceWrapper;
import com.temesoft.fs.S3FileStorageServiceImpl;
import com.temesoft.fs.SftpFileStorageServiceImpl;
import com.temesoft.fs.SystemFileStorageServiceImpl;
import com.temesoft.fs.spring.FileStorageProperties.FileStorageOption;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
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
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.temesoft.fs.spring.FileStorageProperties.PREFIX;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_SINGLETON;
import static org.springframework.util.ObjectUtils.isEmpty;

/**
 * Configuration class setting up a bean registry approach to creating one or more beans {@link FileStorageService}
 * specified by configuration defined in {@link FileStorageProperties}.
 */
@Configuration
public class FileStorageBeanRegistryConfiguration implements BeanDefinitionRegistryPostProcessor, EnvironmentAware, ApplicationContextAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileStorageBeanRegistryConfiguration.class);
    public static final String ENDPOINT_ID = "file-storage";
    private static final String USAGE_MESSAGE =
            "Processing file storage configuration '{}.instances.{}.*' into implementation '{}' setup " +
            "for entity '{}' and id service '{}' as bean '{}'";
    private static final String GENERATING_MESSAGE = "Generating {} {} bean(s)";
    private static final String PROPERTY_NAME_PORTION = ".instances.%s.";

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

    /**
     * Registers custom actuator endpoint describing file storage beans registered.
     * Can be disabled using configuration property: `app.file-storage.endpoint-enabled=false`
     */
    @Component
    @ConditionalOnProperty(value = "${" + PREFIX + ".endpoint-enabled:true}", matchIfMissing = true, havingValue = "true")
    @Endpoint(id = FileStorageBeanRegistryConfiguration.ENDPOINT_ID)
    public static class FileStorageEndpoint implements ApplicationContextAware {

        private ApplicationContext context;

        @Override
        public void setApplicationContext(final ApplicationContext context) throws BeansException {
            this.context = context;
        }

        /**
         * Endpoint read operation, when enabled - accessible via /actuator/file-storage
         */
        @ReadOperation
        @SuppressWarnings("rawtypes")
        public Map<String, Object> viewRegisteredFileStorages() {
            return context.getBeansOfType(FileStorageService.class)
                    .entrySet()
                    .stream()
                    .map((Function<Map.Entry<String, FileStorageService>, Map.Entry<String, Object>>) entry -> {
                        final String serviceClassName;
                        if (entry.getValue() instanceof LoggingFileStorageServiceWrapper<?>) {
                            serviceClassName = ((LoggingFileStorageServiceWrapper<?>) entry.getValue())
                                    .getService().getClass().getName();
                        } else {
                            serviceClassName = entry.getValue().getClass().getName();
                        }
                        return new AbstractMap.SimpleEntry<>(
                                entry.getKey(),
                                Map.of(
                                        "description", entry.getValue().getStorageDescription(),
                                        "storageService", serviceClassName,
                                        "idService", entry.getValue().getFileStorageIdService().getClass().getName()
                                )
                        );
                    }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    @Override
    public void postProcessBeanDefinitionRegistry(final BeanDefinitionRegistry registry) throws BeansException {
        final FileStorageProperties fileStorageProperties = Binder.get(environment)
                .bind(PREFIX, Bindable.of(FileStorageProperties.class))
                .orElse(new FileStorageProperties());
        if (isEmpty(fileStorageProperties.getInstances())) {
            return;
        }
        postProcessInstancesToBeans(registry, fileStorageProperties);
    }

    /**
     * Method for file storage bean creation from configuration provided in {@link FileStorageProperties}
     */
    @VisibleForTesting
    protected void postProcessInstancesToBeans(final BeanDefinitionRegistry registry,
                                               final FileStorageProperties fileStorageProperties) {
        LOGGER.info(
                GENERATING_MESSAGE,
                fileStorageProperties.getInstances().size(),
                FileStorageService.class.getSimpleName()
        );
        fileStorageProperties.getInstances().forEach((key, config) -> {
            final GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
            beanDefinition.setScope(SCOPE_SINGLETON);
            try {
                if (config.getType() == null) {
                    throw new IllegalStateException("Missing configuration for storage type: " +
                                                    PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "type");
                } else if (isEmpty(config.getEntityClass())) {
                    throw new IllegalStateException("Missing configuration for entity class: " +
                                                    PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "entity-class");
                } else if (isEmpty(config.getIdService())) {
                    throw new IllegalStateException("Missing configuration for id service: " +
                                                    PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "id-service");
                } else if (isEmpty(config.getBeanQualifier())) {
                    throw new IllegalStateException("Missing configuration for bean qualifier: " +
                                                    PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "bean-qualifier");
                }
                final Class<?> idService = Class.forName(config.getIdService());
                final Class<?> entityClass = Class.forName(config.getEntityClass());

                if (config.getType() == FileStorageOption.InMemory) {
                    createFileStorageServiceInMemory(beanDefinition, idService);
                } else if (config.getType() == FileStorageOption.System) {
                    createFileStorageServiceSystem(beanDefinition, idService, config, key);
                } else if (config.getType() == FileStorageOption.Sftp) {
                    createFileStorageServiceSftp(beanDefinition, idService, config, key);
                } else if (config.getType() == FileStorageOption.S3) {
                    createFileStorageServiceS3(beanDefinition, idService, config, key);
                } else if (config.getType() == FileStorageOption.GCS) {
                    createFileStorageServiceGcs(beanDefinition, idService, config, key);
                } else if (config.getType() == FileStorageOption.Azure) {
                    createFileStorageServiceAzure(beanDefinition, idService);
                } else if (config.getType() == FileStorageOption.HDFS) {
                    createFileStorageServiceHdfs(beanDefinition, idService);
                } else {
                    throw new IllegalStateException("Not configured to create storage of this type: " + config.getType());
                }

                LOGGER.debug(USAGE_MESSAGE,
                        PREFIX,
                        key,
                        beanDefinition.getBeanClass().getSimpleName(),
                        entityClass.getSimpleName(),
                        idService.getSimpleName(),
                        config.getBeanQualifier()
                );

                beanDefinition.addQualifier(new AutowireCandidateQualifier(config.getBeanQualifier()));
                registry.registerBeanDefinition(config.getBeanQualifier(), beanDefinition);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                     NoSuchMethodException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Generates file storage service for InMemory based on provided config and idService
     */
    private void createFileStorageServiceInMemory(final GenericBeanDefinition beanDefinition,
                                                  final Class<?> idService) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        beanDefinition.setBeanClass(InMemoryFileStorageServiceImpl.class);
        final FileStorageService<?> srv = new LoggingFileStorageServiceWrapper<>(new InMemoryFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance()
        ));
        beanDefinition.setInstanceSupplier(() -> srv);
    }

    /**
     * Generates file storage service for System based on provided config and idService
     */
    @VisibleForTesting
    protected void createFileStorageServiceSystem(final GenericBeanDefinition beanDefinition,
                                                  final Class<?> idService,
                                                  final FileStorageProperties.FileStorageConfig config,
                                                  final String key) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (isEmpty(config.getSystem().getRootLocation())) {
            throw new IllegalStateException("Missing configuration for system root location: " +
                                            PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "system.root-location");
        }
        beanDefinition.setBeanClass(SystemFileStorageServiceImpl.class);
        final FileStorageService<?> srv = new LoggingFileStorageServiceWrapper<>(new SystemFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                Path.of(config.getSystem().getRootLocation())
        ));
        beanDefinition.setInstanceSupplier(() -> srv);
    }

    /**
     * Generates file storage service for SFTP based on provided config and idService
     */
    @VisibleForTesting
    protected void createFileStorageServiceSftp(final GenericBeanDefinition beanDefinition,
                                                final Class<?> idService,
                                                final FileStorageProperties.FileStorageConfig config,
                                                final String key) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (isEmpty(config.getSftp().getRemoteHost())) {
            throw new IllegalStateException("Missing configuration for sftp remote host: " +
                                            PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "sftp.remote-host");
        } else if (config.getSftp().getRemotePort() <= 0) {
            throw new IllegalStateException("Missing configuration for sftp remote port: " +
                                            PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "sftp.remote-port");
        } else if (isEmpty(config.getSftp().getUsername())) {
            throw new IllegalStateException("Missing configuration for sftp username: " +
                                            PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "sftp.username");
        } else if (isEmpty(config.getSftp().getPassword())) {
            throw new IllegalStateException("Missing configuration for sftp password: " +
                                            PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "sftp.password");
        } else if (isEmpty(config.getSftp().getRootDirectory())) {
            throw new IllegalStateException("Missing configuration for sftp root directory: " +
                                            PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "sftp.root-directory");
        }
        beanDefinition.setBeanClass(SftpFileStorageServiceImpl.class);
        final FileStorageService<?> srv = new LoggingFileStorageServiceWrapper<>(new SftpFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                config.getSftp().getRemoteHost(),
                config.getSftp().getRemotePort(),
                config.getSftp().getUsername(),
                config.getSftp().getPassword(),
                config.getSftp().getRootDirectory(),
                config.getSftp().getConfigProperties()
        ));
        beanDefinition.setInstanceSupplier(() -> srv);
    }

    /**
     * Generates file storage service for S3 based on provided config and idService
     */
    @VisibleForTesting
    protected void createFileStorageServiceS3(final GenericBeanDefinition beanDefinition,
                                              final Class<?> idService,
                                              final FileStorageProperties.FileStorageConfig config,
                                              final String key) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (isEmpty(config.getS3().getBucketName())) {
            throw new IllegalStateException("Missing configuration for S3 bucket name: " +
                                            PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "s3.bucket-name");
        }
        beanDefinition.setBeanClass(S3FileStorageServiceImpl.class);
        final FileStorageService<?> srv = new LoggingFileStorageServiceWrapper<>(new S3FileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                context.getBean(S3Client.class),
                config.getS3().getBucketName()
        ));
        beanDefinition.setInstanceSupplier(() -> srv);
    }

    /**
     * Generates file storage service for GCS based on provided config and idService
     */
    @VisibleForTesting
    protected void createFileStorageServiceGcs(final GenericBeanDefinition beanDefinition,
                                               final Class<?> idService,
                                               final FileStorageProperties.FileStorageConfig config,
                                               final String key) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (isEmpty(config.getGcs().getBucketName())) {
            throw new IllegalStateException("Missing configuration for GCS bucket name: " +
                                            PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "gcs.bucket-name");
        }
        beanDefinition.setBeanClass(GcsFileStorageServiceImpl.class);
        final FileStorageService<?> srv = new LoggingFileStorageServiceWrapper<>(new GcsFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                context.getBean(Storage.class),
                config.getGcs().getBucketName()
        ));
        beanDefinition.setInstanceSupplier(() -> srv);
    }

    /**
     * Generates file storage service for Azure based on provided config and idService
     */
    private void createFileStorageServiceAzure(final GenericBeanDefinition beanDefinition,
                                               final Class<?> idService) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        beanDefinition.setBeanClass(AzureFileStorageServiceImpl.class);
        final FileStorageService<?> srv = new LoggingFileStorageServiceWrapper<>(new AzureFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                context.getBean(BlobContainerClient.class)
        ));
        beanDefinition.setInstanceSupplier(() -> srv);
    }

    /**
     * Generates file storage service for HDFS based on provided config and idService
     */
    private void createFileStorageServiceHdfs(final GenericBeanDefinition beanDefinition,
                                              final Class<?> idService) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        beanDefinition.setBeanClass(HdfsFileStorageServiceImpl.class);
        final FileStorageService<?> srv = new LoggingFileStorageServiceWrapper<>(new HdfsFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                context.getBean(FileSystem.class)
        ));
        beanDefinition.setInstanceSupplier(() -> srv);
    }
}
