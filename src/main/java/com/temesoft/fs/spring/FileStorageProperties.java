package com.temesoft.fs.spring;

import com.google.common.collect.Maps;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Properties;

/**
 * Config properties holding setup details for file storage system (see: {@link FileStorageBeanRegistryConfiguration}).
 * Below is an example of possible configurations:
 * <br/>
 * <pre>
 * # Enables actuator endpoint "file-storage" (enabled by default)
 * app.file-storage.endpoint-enabled=true
 *
 * app.file-storage.instances.widget-mem.type=InMemory
 * app.file-storage.instances.widget-mem.bean-qualifier=widgetFileStorage
 * app.file-storage.instances.widget-mem.entity-class=org.some.where.Widget
 * # idService should implement com.temesoft.fs.FileStorageIdService&lt;Widget&gt;
 * app.file-storage.instances.widget-mem.id-service=org.some.where.WidgetFileStorageIdService
 *
 * # Using encryption
 * app.file-storage.instances.trinket-sys.type=System
 * app.file-storage.instances.trinket-sys.bean-qualifier=trinketSysFileStorage
 * app.file-storage.instances.trinket-sys.entity-class=org.some.where.Trinket
 * app.file-storage.instances.trinket-sys.id-service=org.some.where.TrinketFileStorageIdService
 * app.file-storage.instances.trinket-sys.system.rootLocation=/tmp/test-file-storage
 * app.file-storage.instances.trinket-sys.encryption.enabled=true
 * app.file-storage.instances.trinket-sys.encryption.algorithm=AES_GCM_CHUNKED
 * app.file-storage.instances.trinket-sys.encryption.chunk-size=65536
 * app.file-storage.instances.trinket-sys.encryption.key-id=main
 * app.file-storage.instances.trinket-sys.encryption.base64-key=V3dYQjZ2cWJ5QjVyR0t3Y0tVS2N6N0pqZGRxNzR2cVg=
 *
 * app.file-storage.instances.trinket-sftp.type=Sftp
 * app.file-storage.instances.trinket-sftp.bean-qualifier=trinketSftpFileStorage
 * app.file-storage.instances.trinket-sftp.entity-class=org.some.where.Trinket
 * app.file-storage.instances.trinket-sftp.id-service=org.some.where.TrinketFileStorageIdService
 * app.file-storage.instances.trinket-sftp.sftp.remote-host=127.0.0.1
 * app.file-storage.instances.trinket-sftp.sftp.remote-port=12345
 * app.file-storage.instances.trinket-sftp.sftp.username=username
 * app.file-storage.instances.trinket-sftp.sftp.password=password
 * app.file-storage.instances.trinket-sftp.sftp.root-directory=/tmp/test-file-storage
 * # Additional configuration for jsch sftp (for example "StrictHostKeyChecking=no")
 * app.file-storage.instances.trinket-sftp.sftp.config-properties.StrictHostKeyChecking=no
 *
 * # S3 integration uses encryption
 * # S3 integration will require a bean software.amazon.awssdk.services.s3.S3Client
 * app.file-storage.instances.trinket-s3.type=S3
 * app.file-storage.instances.trinket-s3.entity-class=org.some.where.Trinket
 * app.file-storage.instances.trinket-s3.id-service=org.some.where.TrinketFileStorageIdService
 * app.file-storage.instances.trinket-s3.bean-qualifier=trinketS3FileStorage
 * app.file-storage.instances.trinket-s3.s3.bucket-name=test-bucket
 * app.file-storage.instances.trinket-s3.encryption.enabled=true
 * app.file-storage.instances.trinket-s3.encryption.algorithm=AES_GCM_CHUNKED
 * app.file-storage.instances.trinket-s3.encryption.chunk-size=65536
 * app.file-storage.instances.trinket-s3.encryption.key-id=main
 * app.file-storage.instances.trinket-s3.encryption.base64-key=V3dYQjZ2cWJ5QjVyR0t3Y0tVS2N6N0pqZGRxNzR2cVg=
 *
 * # GCS integration will require a bean com.google.cloud.storage.Storage
 * app.file-storage.instances.trinket-gcs.type=GCS
 * app.file-storage.instances.trinket-gcs.entity-class=org.some.where.Trinket
 * app.file-storage.instances.trinket-gcs.id-service=org.some.where.TrinketFileStorageIdService
 * app.file-storage.instances.trinket-gcs.bean-qualifier=trinketGcsFileStorage
 * app.file-storage.instances.trinket-gcs.gcs.bucket-name=test-bucket
 *
 * # Azure integration will require a bean com.azure.storage.blob.BlobContainerClient
 * app.file-storage.instances.trinket-azure.type=Azure
 * app.file-storage.instances.trinket-azure.bean-qualifier=trinketAzureFileStorage
 * app.file-storage.instances.trinket-azure.entity-class=org.some.where.Trinket
 * app.file-storage.instances.trinket-azure.id-service=org.some.where.TrinketFileStorageIdService
 * app.file-storage.instances.trinket-azure.azure.bucket-name=test-bucket
 *
 * # HDFS integration will require a bean org.apache.hadoop.fs.FileSystem
 * app.file-storage.instances.trinket-hdfs.type=HDFS
 * app.file-storage.instances.trinket-hdfs.bean-qualifier=trinketHdfsFileStorage
 * app.file-storage.instances.trinket-hdfs.entity-class=org.some.where.Trinket
 * app.file-storage.instances.trinket-hdfs.id-service=org.some.where.TrinketFileStorageIdService
 * app.file-storage.instances.trinket-hdfs.azure.bucket-name=test-bucket
 * </pre>
 */
@Component
@ConfigurationProperties(prefix = FileStorageProperties.PREFIX)
public class FileStorageProperties {

    public static final String PREFIX = "app.file-storage";

    private Map<String, FileStorageConfig> instances = Maps.newHashMap();
    private boolean endpointEnabled;

    public Map<String, FileStorageConfig> getInstances() {
        return instances;
    }

    public void setInstances(final Map<String, FileStorageConfig> instances) {
        this.instances = instances;
    }

    public boolean isEndpointEnabled() {
        return endpointEnabled;
    }

    public void setEndpointEnabled(final boolean endpointEnabled) {
        this.endpointEnabled = endpointEnabled;
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
        private EncryptionProperties encryption = new EncryptionProperties();

        private SystemFileStorageProperties system = new SystemFileStorageProperties();
        private SftpFileStorageProperties sftp = new SftpFileStorageProperties();
        private S3FileStorageProperties s3 = new S3FileStorageProperties();
        private GcsFileStorageProperties gcs = new GcsFileStorageProperties();

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

        public EncryptionProperties getEncryption() {
            return encryption;
        }

        public void setEncryption(final EncryptionProperties encryption) {
            this.encryption = encryption;
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
        private String privateKeyPath;
        private String passphrase;
        private String rootDirectory;
        private Properties configProperties = new Properties();

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

        public String getPrivateKeyPath() {
            return privateKeyPath;
        }

        public void setPrivateKeyPath(final String privateKeyPath) {
            this.privateKeyPath = privateKeyPath;
        }

        public String getPassphrase() {
            return passphrase;
        }

        public void setPassphrase(final String passphrase) {
            this.passphrase = passphrase;
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

    public static class EncryptionProperties {

        /**
         * Enable transparent encryption wrapper.
         */
        private boolean enabled = false;

        /**
         * Currently supported: AES_GCM_CHUNKED
         */
        private String algorithm = "AES_GCM_CHUNKED";

        /**
         * Plaintext chunk size in bytes.
         */
        private int chunkSize = 65536;

        /**
         * Key identifier stored in encrypted header.
         */
        private String keyId = "default";

        /**
         * Base64-encoded AES key (128/192/256 bit).
         */
        private String base64Key;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(final boolean enabled) {
            this.enabled = enabled;
        }

        public String getAlgorithm() {
            return algorithm;
        }

        public void setAlgorithm(final String algorithm) {
            this.algorithm = algorithm;
        }

        public int getChunkSize() {
            return chunkSize;
        }

        public void setChunkSize(final int chunkSize) {
            this.chunkSize = chunkSize;
        }

        public String getKeyId() {
            return keyId;
        }

        public void setKeyId(final String keyId) {
            this.keyId = keyId;
        }

        public String getBase64Key() {
            return base64Key;
        }

        public void setBase64Key(final String base64Key) {
            this.base64Key = base64Key;
        }
    }
}
