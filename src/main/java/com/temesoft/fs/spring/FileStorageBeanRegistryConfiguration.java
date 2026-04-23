package com.temesoft.fs.spring;

import com.azure.storage.blob.BlobContainerClient;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.temesoft.fs.AesGcmChunkedFileStorageCryptor;
import com.temesoft.fs.AzureFileStorageServiceImpl;
import com.temesoft.fs.EncryptingFileStorageServiceWrapper;
import com.temesoft.fs.FileStorageCryptor;
import com.temesoft.fs.FileStorageIdService;
import com.temesoft.fs.FileStorageService;
import com.temesoft.fs.FileStorageServiceWrapper;
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
                        final String serviceClassName = FileStorageServiceWrapper
                                .unwrap(entry.getValue())
                                .getClass()
                                .getName();
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
                    createFileStorageServiceInMemory(beanDefinition, idService, config);
                } else if (config.getType() == FileStorageOption.System) {
                    createFileStorageServiceSystem(beanDefinition, idService, config, key);
                } else if (config.getType() == FileStorageOption.Sftp) {
                    createFileStorageServiceSftp(beanDefinition, idService, config, key);
                } else if (config.getType() == FileStorageOption.S3) {
                    createFileStorageServiceS3(beanDefinition, idService, config, key);
                } else if (config.getType() == FileStorageOption.GCS) {
                    createFileStorageServiceGcs(beanDefinition, idService, config, key);
                } else if (config.getType() == FileStorageOption.Azure) {
                    createFileStorageServiceAzure(beanDefinition, idService, config);
                } else if (config.getType() == FileStorageOption.HDFS) {
                    createFileStorageServiceHdfs(beanDefinition, idService, config);
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
     * Conditionally wraps a file storage service with encryption capabilities based on the provided configuration.
     * <p>
     * If encryption is enabled and valid, it returns an {@link EncryptingFileStorageServiceWrapper} using
     * the AES/GCM chunked algorithm. If encryption is disabled, the original service is returned.
     *
     * @param <T>     The type of file metadata handled by the service
     * @param service The base storage service to potentially wrap
     * @param props   The storage configuration containing encryption settings
     * @return An encrypted wrapper if enabled; otherwise, the original service
     * @throws IllegalArgumentException if the algorithm is unsupported, the key is missing, or chunk size is invalid
     */
    private <T> FileStorageService<T> maybeWrapWithEncryption(
            final FileStorageService<T> service,
            final FileStorageProperties.FileStorageConfig props
    ) {
        final FileStorageProperties.EncryptionProperties encryption = props.getEncryption();
        if (encryption == null || !encryption.isEnabled()) {
            return service;
        }

        final String algorithm = encryption.getAlgorithm();
        if (!"AES_GCM_CHUNKED".equalsIgnoreCase(algorithm)) {
            throw new IllegalArgumentException("Unsupported encryption algorithm: " + algorithm);
        }
        if (isEmpty(encryption.getBase64Key())) {
            throw new IllegalArgumentException("Encryption is enabled but base64Key is not configured");
        }
        if (encryption.getChunkSize() <= 0) {
            throw new IllegalArgumentException("Encryption chunkSize must be > 0");
        }

        final FileStorageCryptor cryptor = new AesGcmChunkedFileStorageCryptor(
                encryption.getKeyId(),
                encryption.getBase64Key()
        );

        return new EncryptingFileStorageServiceWrapper<>(service, cryptor, encryption.getChunkSize());
    }

    /**
     * Wraps a file storage service with a logging layer to track storage operations.
     *
     * @param <T>     The type of file metadata handled by the service
     * @param service The base storage service to wrap
     * @return A new {@link LoggingFileStorageServiceWrapper} decorating the original service
     */
    private <T> FileStorageService<T> wrapWithLogging(final FileStorageService<T> service) {
        return new LoggingFileStorageServiceWrapper<>(service);
    }

    /**
     * Generates file storage service for InMemory based on provided config and idService
     */
    private void createFileStorageServiceInMemory(final GenericBeanDefinition beanDefinition,
                                                  final Class<?> idService,
                                                  final FileStorageProperties.FileStorageConfig config) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        beanDefinition.setBeanClass(InMemoryFileStorageServiceImpl.class);
        final FileStorageService<?> srv = new InMemoryFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance()
        );
        beanDefinition.setInstanceSupplier(() -> wrap(srv, config));
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
        final FileStorageService<?> srv = new SystemFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                Path.of(config.getSystem().getRootLocation())
        );
        beanDefinition.setInstanceSupplier(() -> wrap(srv, config));
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
        } else if (isEmpty(config.getSftp().getPassword()) && isEmpty(config.getSftp().getPrivateKeyPath())) {
            throw new IllegalStateException("Missing configuration for sftp password or private key: " +
                    PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "sftp.password/private-key-path");
        } else if (isEmpty(config.getSftp().getRootDirectory())) {
            throw new IllegalStateException("Missing configuration for sftp root directory: " +
                    PREFIX + PROPERTY_NAME_PORTION.formatted(key) + "sftp.root-directory");
        }
        beanDefinition.setBeanClass(SftpFileStorageServiceImpl.class);
        final FileStorageService<?> srv = new SftpFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                config.getSftp().getRemoteHost(),
                config.getSftp().getRemotePort(),
                config.getSftp().getUsername(),
                config.getSftp().getPassword(),
                config.getSftp().getPrivateKeyPath(),
                config.getSftp().getPassphrase(),
                config.getSftp().getRootDirectory(),
                config.getSftp().getConfigProperties()
        );
        beanDefinition.setInstanceSupplier(() -> wrap(srv, config));
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
        final FileStorageService<?> srv = new S3FileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                context.getBean(S3Client.class),
                config.getS3().getBucketName()
        );
        beanDefinition.setInstanceSupplier(() -> wrap(srv, config));
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
        final FileStorageService<?> srv = new GcsFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                context.getBean(Storage.class),
                config.getGcs().getBucketName()
        );
        beanDefinition.setInstanceSupplier(() -> wrap(srv, config));
    }

    /**
     * Generates file storage service for Azure based on provided config and idService
     */
    private void createFileStorageServiceAzure(final GenericBeanDefinition beanDefinition,
                                               final Class<?> idService,
                                               final FileStorageProperties.FileStorageConfig config) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        beanDefinition.setBeanClass(AzureFileStorageServiceImpl.class);
        final FileStorageService<?> srv = new AzureFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                context.getBean(BlobContainerClient.class)
        );
        beanDefinition.setInstanceSupplier(() -> wrap(srv, config));
    }

    /**
     * Generates file storage service for HDFS based on provided config and idService
     */
    private void createFileStorageServiceHdfs(final GenericBeanDefinition beanDefinition,
                                              final Class<?> idService,
                                              final FileStorageProperties.FileStorageConfig config) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        beanDefinition.setBeanClass(HdfsFileStorageServiceImpl.class);
        final FileStorageService<?> srv = new HdfsFileStorageServiceImpl<>(
                (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                context.getBean(FileSystem.class)
        );
        beanDefinition.setInstanceSupplier(() -> wrap(srv, config));
    }

    /**
     * Composes the final storage service by applying a chain of wrappers in a specific order.
     * This method orchestrates the layering of service functionality:
     * <ol>
     *     <li>Applies encryption wrapping if configured (Layer 2).</li>
     *     <li>Applies logging wrapping (Layer 3).</li>
     * </ol>
     *
     * @param srv    The base storage service to be decorated
     * @param config The configuration properties defining which layers to apply
     * @return A decorated storage service instance incorporating encryption and logging
     */
    private FileStorageService<?> wrap(final FileStorageService<?> srv, final FileStorageProperties.FileStorageConfig config) {
        final FileStorageService<?> srvLevelTwo = maybeWrapWithEncryption(srv, config);
        return wrapWithLogging(srvLevelTwo);
    }
}
