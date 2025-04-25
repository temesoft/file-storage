package com.temesoft.fs.spring;

import com.azure.storage.blob.BlobContainerClient;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Maps;
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
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.springframework.util.ObjectUtils.isEmpty;

/**
 * Configuration class setting up a bean factory approach to creating one or more beans {@link FileStorageService}
 * specified by configuration defined in {@link FileStorageProperties}.
 */
@Configuration
public class FileStorageBeanFactoryConfiguration implements FactoryBean<FileStorageIdService<?>> {

    public static final String ENDPOINT_ID = "file-storage";

    private static final Logger LOGGER = LoggerFactory.getLogger(FileStorageBeanFactoryConfiguration.class);
    private static final String USAGE_MESSAGE =
            "For file storage configuration '{}.file-storage.{}.*' an implementation '{}' is being configured " +
            "for entity '{}' and id service '{}' as bean '{}'";
    private static final String GENERATING_MESSAGE = "Generating {} {} bean(s)";

    private static final Map<String, FileStorageService<?>> BEAN_MAP = Maps.newHashMap();

    @Autowired
    private FileStorageProperties fileStorageProperties;
    @Autowired
    private BeanFactory beanFactory;

    @Component
    @Endpoint(id = FileStorageBeanFactoryConfiguration.ENDPOINT_ID)
    static class FileStorageEndpointConfiguration {
        /**
         * Endpoint read operation, when enabled - accessible via /actuator/file-storage
         */
        @ReadOperation
        public Map<String, FileStorageService<?>> viewFileStorages() {
            return BEAN_MAP;
        }
    }

    @Override
    public FileStorageIdService<?> getObject() {
        final BeanDefinitionRegistry registry = (BeanDefinitionRegistry) beanFactory;
        BEAN_MAP.clear();
        if (isEmpty(fileStorageProperties.getFileStorage())) {
            return genericFileStorageIdService;
        }
        LOGGER.info(
                GENERATING_MESSAGE,
                fileStorageProperties.getFileStorage().size(),
                FileStorageService.class.getSimpleName()
        );
        fileStorageProperties.getFileStorage().forEach((key, value) -> {
            final GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
            beanDefinition.setScope(BeanDefinition.SCOPE_SINGLETON);
            try {
                if (value.getType() == null) {
                    throw new IllegalStateException("Missing configuration for storage type");
                } else if (isEmpty(value.getEntityClass())) {
                    throw new IllegalStateException("Missing configuration for entity class");
                } else if (isEmpty(value.getIdService())) {
                    throw new IllegalStateException("Missing configuration for id service");
                } else if (isEmpty(value.getBeanQualifier())) {
                    throw new IllegalStateException("Missing configuration for bean qualifier");
                }
                final Class<?> idService = Class.forName(value.getIdService());
                final Class<?> entityClass = Class.forName(value.getEntityClass());
                if (value.getType() == FileStorageProperties.FileStorageOption.InMemory) {
                    beanDefinition.setBeanClass(InMemoryFileStorageServiceImpl.class);
                    final FileStorageService<?> srv = new InMemoryFileStorageServiceImpl<>(
                            (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance()
                    );
                    beanDefinition.setInstanceSupplier(() -> srv);
                } else if (value.getType() == FileStorageProperties.FileStorageOption.System) {
                    if (isEmpty(value.getSystem().getRootLocation())) {
                        throw new IllegalStateException("Missing configuration for system root location");
                    }
                    beanDefinition.setBeanClass(SystemFileStorageServiceImpl.class);
                    final FileStorageService<?> srv = new SystemFileStorageServiceImpl<>(
                            (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                            Path.of(value.getSystem().getRootLocation())
                    );
                    beanDefinition.setInstanceSupplier(() -> srv);
                } else if (value.getType() == FileStorageProperties.FileStorageOption.Sftp) {
                    if (isEmpty(value.getSftp().getRemoteHost())) {
                        throw new IllegalStateException("Missing configuration for sftp remote host");
                    } else if (isEmpty(value.getSftp().getRemotePort())) {
                        throw new IllegalStateException("Missing configuration for sftp remote port");
                    } else if (isEmpty(value.getSftp().getUsername())) {
                        throw new IllegalStateException("Missing configuration for sftp username");
                    } else if (isEmpty(value.getSftp().getPassword())) {
                        throw new IllegalStateException("Missing configuration for sftp password");
                    } else if (isEmpty(value.getSftp().getRootDirectory())) {
                        throw new IllegalStateException("Missing configuration for sftp root directory");
                    }
                    beanDefinition.setBeanClass(SftpFileStorageServiceImpl.class);
                    final FileStorageService<?> srv = new SftpFileStorageServiceImpl<>(
                            (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                            value.getSftp().getRemoteHost(),
                            value.getSftp().getRemotePort(),
                            value.getSftp().getUsername(),
                            value.getSftp().getPassword(),
                            value.getSftp().getRootDirectory(),
                            value.getSftp().getConfigProperties()
                    );
                    beanDefinition.setInstanceSupplier(() -> srv);
                } else if (value.getType() == FileStorageProperties.FileStorageOption.S3) {
                    if (isEmpty(value.getS3().getBucketName())) {
                        throw new IllegalStateException("Missing configuration for S3 bucket name");
                    }
                    beanDefinition.setBeanClass(S3FileStorageServiceImpl.class);
                    final FileStorageService<?> srv = new S3FileStorageServiceImpl<>(
                            (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                            beanFactory.getBean(S3Client.class),
                            value.getS3().getBucketName()
                    );
                    beanDefinition.setInstanceSupplier(() -> srv);
                } else if (value.getType() == FileStorageProperties.FileStorageOption.GCS) {
                    if (isEmpty(value.getGcs().getBucketName())) {
                        throw new IllegalStateException("Missing configuration for GCS bucket name");
                    }
                    beanDefinition.setBeanClass(GcsFileStorageServiceImpl.class);
                    final FileStorageService<?> srv = new GcsFileStorageServiceImpl<>(
                            (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                            beanFactory.getBean(Storage.class),
                            value.getGcs().getBucketName()
                    );
                    beanDefinition.setInstanceSupplier(() -> srv);
                } else if (value.getType() == FileStorageProperties.FileStorageOption.Azure) {
                    beanDefinition.setBeanClass(AzureFileStorageServiceImpl.class);
                    final FileStorageService<?> srv = new AzureFileStorageServiceImpl<>(
                            (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                            beanFactory.getBean(BlobContainerClient.class)
                    );
                    beanDefinition.setInstanceSupplier(() -> srv);
                } else if (value.getType() == FileStorageProperties.FileStorageOption.HDFS) {
                    beanDefinition.setBeanClass(HdfsFileStorageServiceImpl.class);
                    final FileStorageService<?> srv = new HdfsFileStorageServiceImpl<>(
                            (FileStorageIdService<?>) idService.getDeclaredConstructor().newInstance(),
                            beanFactory.getBean(FileSystem.class)
                    );
                    beanDefinition.setInstanceSupplier(() -> srv);
                } else {
                    throw new IllegalStateException("Not configured to create storage of this type: " + value.getType());
                }

                LOGGER.debug(USAGE_MESSAGE,
                        FileStorageProperties.PREFIX,
                        key,
                        beanDefinition.getBeanClass().getSimpleName(),
                        entityClass.getSimpleName(),
                        idService.getSimpleName(),
                        value.getBeanQualifier()
                );

                if (beanDefinition.getInstanceSupplier() != null
                    && beanDefinition.getInstanceSupplier().get() != null) {
                    BEAN_MAP.put(value.getBeanQualifier(), (FileStorageService<?>) beanDefinition.getInstanceSupplier().get());
                }

                beanDefinition.addQualifier(new AutowireCandidateQualifier(value.getBeanQualifier()));
                registry.registerBeanDefinition(value.getBeanQualifier(), beanDefinition);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                     NoSuchMethodException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });

        return genericFileStorageIdService;
    }

    @Override
    public Class<?> getObjectType() {
        return FileStorageIdService.class;
    }

    private final FileStorageIdService<?> genericFileStorageIdService =
            value -> new FileStorageId<>(value) {
                @Override
                public String generatePath() {
                    return value.toString();
                }
            };

    /**
     * This configuration triggers Spring to execute this BeanFactory method `getObject(...)`
     * and pre-create our service beans {@link FileStorageService} instead ;-)
     */
    @Configuration
    static class BeanConfigurator {
        @Autowired(required = false)
        private List<FileStorageIdService<?>> services;
    }
}
