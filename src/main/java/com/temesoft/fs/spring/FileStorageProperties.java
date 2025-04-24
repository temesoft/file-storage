package com.temesoft.fs.spring;

import com.google.common.collect.Maps;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Properties;

/**
 * Config properties holding setup details for file storage system (see: {@link FileStorageBeanFactoryConfiguration}).
 * Below is an example of possible configurations:
 * <br/>
 * <pre>
 * app.file-storage.widget-mem.type=InMemory
 * app.file-storage.widget-mem.beanQualifier=widgetFileStorage
 * app.file-storage.widget-mem.entityClass=org.some.where.Widget
 * # idService should implement com.temesoft.fs.FileStorageIdService&lt;Widget&gt;
 * app.file-storage.widget-mem.idService=org.some.where.WidgetFileStorageIdService
 *
 * app.file-storage.trinket-sys.type=System
 * app.file-storage.trinket-sys.beanQualifier=trinketSysFileStorage
 * app.file-storage.trinket-sys.entityClass=org.some.where.Trinket
 * app.file-storage.trinket-sys.idService=org.some.where.TrinketFileStorageIdService
 * app.file-storage.trinket-sys.system.rootLocation=/tmp/test-file-storage
 *
 * app.file-storage.trinket-sftp.type=Sftp
 * app.file-storage.trinket-sftp.beanQualifier=trinketSftpFileStorage
 * app.file-storage.trinket-sftp.entityClass=org.some.where.Trinket
 * app.file-storage.trinket-sftp.idService=org.some.where.TrinketFileStorageIdService
 * app.file-storage.trinket-sftp.sftp.remoteHost=127.0.0.1
 * app.file-storage.trinket-sftp.sftp.remotePort=12345
 * app.file-storage.trinket-sftp.sftp.username=username
 * app.file-storage.trinket-sftp.sftp.password=password
 * app.file-storage.trinket-sftp.sftp.rootDirectory=/tmp/test-file-storage
 * app.file-storage.trinket-sftp.sftp.configProperties=
 *
 * # S3 integration will require a bean software.amazon.awssdk.services.s3.S3Client
 * app.file-storage.trinket-s3.type=S3
 * app.file-storage.trinket-s3.entityClass=org.some.where.Trinket
 * app.file-storage.trinket-s3.idService=org.some.where.TrinketFileStorageIdService
 * app.file-storage.trinket-s3.beanQualifier=trinketS3FileStorage
 * app.file-storage.trinket-s3.s3.bucketName=test-bucket
 *
 * # GCS integration will require a bean com.google.cloud.storage.Storage
 * app.file-storage.trinket-gcs.type=GCS
 * app.file-storage.trinket-gcs.entityClass=org.some.where.Trinket
 * app.file-storage.trinket-gcs.idService=org.some.where.TrinketFileStorageIdService
 * app.file-storage.trinket-gcs.beanQualifier=trinketGcsFileStorage
 * app.file-storage.trinket-gcs.gcs.bucketName=test-bucket
 *
 * # Azure integration will require a bean com.azure.storage.blob.BlobContainerClient
 * app.file-storage.trinket-azure.type=Azure
 * app.file-storage.trinket-azure.beanQualifier=trinketAzureFileStorage
 * app.file-storage.trinket-azure.entityClass=org.some.where.Trinket
 * app.file-storage.trinket-azure.idService=org.some.where.TrinketFileStorageIdService
 * app.file-storage.trinket-azure.azure.bucketName=test-bucket
 *
 * # HDFS integration will require a bean org.apache.hadoop.fs.FileSystem
 * app.file-storage.trinket-hdfs.type=HDFS
 * app.file-storage.trinket-hdfs.beanQualifier=trinketHdfsFileStorage
 * app.file-storage.trinket-hdfs.entityClass=org.some.where.Trinket
 * app.file-storage.trinket-hdfs.idService=org.some.where.TrinketFileStorageIdService
 * app.file-storage.trinket-hdfs.azure.bucketName=test-bucket
 * </pre>
 */
@Component
@ConfigurationProperties(prefix = FileStorageProperties.PREFIX)
public class FileStorageProperties {

    public static final String PREFIX = "app";

    private Map<String, FileStorageConfig> fileStorage = Maps.newHashMap();

    public Map<String, FileStorageConfig> getFileStorage() {
        return fileStorage;
    }

    public void setFileStorage(final Map<String, FileStorageConfig> fileStorage) {
        this.fileStorage = fileStorage;
    }

    public enum FileStorageOption {
        System,
        InMemory,
        Sftp,
        S3,
        GCS,
        HDFS,
        Azure
    }

    public static class FileStorageConfig {
        private String entityClass;
        private String idService;
        private FileStorageOption type;
        private String beanQualifier;

        private SystemFileStorageProperties system;
        private SftpFileStorageProperties sftp;
        private S3FileStorageProperties s3;
        private GcsFileStorageProperties gcs;

        public String getEntityClass() {
            return entityClass;
        }

        public void setEntityClass(final String entityClass) {
            this.entityClass = entityClass;
        }

        public String getIdService() {
            return idService;
        }

        public void setIdService(final String idService) {
            this.idService = idService;
        }

        public FileStorageOption getType() {
            return type;
        }

        public void setType(final FileStorageOption type) {
            this.type = type;
        }

        public String getBeanQualifier() {
            return beanQualifier;
        }

        public void setBeanQualifier(final String beanQualifier) {
            this.beanQualifier = beanQualifier;
        }

        public SystemFileStorageProperties getSystem() {
            return system;
        }

        public void setSystem(final SystemFileStorageProperties system) {
            this.system = system;
        }

        public SftpFileStorageProperties getSftp() {
            return sftp;
        }

        public void setSftp(final SftpFileStorageProperties sftp) {
            this.sftp = sftp;
        }

        public S3FileStorageProperties getS3() {
            return s3;
        }

        public void setS3(final S3FileStorageProperties s3) {
            this.s3 = s3;
        }

        public GcsFileStorageProperties getGcs() {
            return gcs;
        }

        public void setGcs(final GcsFileStorageProperties gcs) {
            this.gcs = gcs;
        }
    }

    public static class SystemFileStorageProperties {
        private String rootLocation;

        public String getRootLocation() {
            return rootLocation;
        }

        public void setRootLocation(final String rootLocation) {
            this.rootLocation = rootLocation;
        }
    }

    public static class AzureFileStorageProperties {

    }

    public static class SftpFileStorageProperties {
        private String remoteHost;
        private int remotePort;
        private String username;
        private String password;
        private String rootDirectory;
        private Properties configProperties;

        public String getRemoteHost() {
            return remoteHost;
        }

        public void setRemoteHost(final String remoteHost) {
            this.remoteHost = remoteHost;
        }

        public int getRemotePort() {
            return remotePort;
        }

        public void setRemotePort(final int remotePort) {
            this.remotePort = remotePort;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(final String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(final String password) {
            this.password = password;
        }

        public String getRootDirectory() {
            return rootDirectory;
        }

        public void setRootDirectory(final String rootDirectory) {
            this.rootDirectory = rootDirectory;
        }

        public Properties getConfigProperties() {
            return configProperties;
        }

        public void setConfigProperties(final Properties configProperties) {
            this.configProperties = configProperties;
        }
    }

    public static class S3FileStorageProperties {
        private String bucketName;

        public String getBucketName() {
            return bucketName;
        }

        public void setBucketName(final String bucketName) {
            this.bucketName = bucketName;
        }
    }

    public static class GcsFileStorageProperties {
        private String bucketName;

        public String getBucketName() {
            return bucketName;
        }

        public void setBucketName(final String bucketName) {
            this.bucketName = bucketName;
        }
    }
}
