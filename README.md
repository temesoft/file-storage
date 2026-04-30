![License](https://img.shields.io/github/license/temesoft/file-storage)
![Java](https://img.shields.io/badge/Java-17%2B-blue)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.temesoft/file-storage.svg)](https://central.sonatype.com/artifact/io.github.temesoft/file-storage)
[![Java CI](https://github.com/temesoft/file-storage/actions/workflows/main.yml/badge.svg)](https://github.com/temesoft/file-storage/actions/workflows/main.yml)
[![Javadoc](https://javadoc.io/badge2/io.github.temesoft/file-storage/javadoc.svg)](https://javadoc.io/doc/io.github.temesoft/file-storage)
<img src='https://raw.githubusercontent.com/temesoft/file-storage/refs/heads/main/jacoco.svg' alt='Test coverage (jacoco)' title='Test coverage (jacoco)'>

# file-storage #

> File storage library is a simple, lightweight and extensible Java library that provides a unified 
> interface for working with different storage backends - from local filesystems to major cloud providers.

------

### Key Features
- Flexible ID & Path generation (based on entity attributes)
- Multiple storage backends (local, AWS S3, Azure, GCS, SFTP, HDFS, In-memory)
- Simple API using `InputStream` or `byte[]`
- Range reads (support for partial content / streaming)
- Transparent Data Encryption (AES-GCM chunked encryption layer)
- Service Layering (Decorator pattern for adding logging, encryption, etc.)
- Spring-Boot integration with programmatic and property based configuration
- Optional decorators/wrappers for Metrics and Observation
- Slf4j logging wrapper for detailed debug output
- Easy integration interface:
    - [FileStorageService](src/main/java/com/temesoft/fs/FileStorageService.java)
- File storage id providers include:
    - [UUID](src/main/java/com/temesoft/fs/UUIDFileStorageId.java)
    - [Ksuid](src/main/java/com/temesoft/fs/KsuidFileStorageId.java)
    - [FileStorageId](src/main/java/com/temesoft/fs/FileStorageId.java) - abstract provider for custom id

------

### Supported Storage Implementations

| Storage Type                   | Library / SDK              | Notes                                  |
|--------------------------------|----------------------------|----------------------------------------|
| **Local File System**          | `java.nio`                 | Simple local persistence               |
| **Amazon AWS S3**              | AWS SDK v2.x               | AWS S3 cloud-native storage            |
| **Google Cloud Storage (GCS)** | GCS SDK v2.x               | GCP GCS cloud-native storage           |
| **Azure Blob Storage**         | `azure-storage-blob v12.x` | Azure cloud-native storage             |
| **SFTP**                       | `jsch v0.2.x`              | Remote secure file transfer            |
| **HDFS**                       | Hadoop v3.x                | Distributed file system support        |
| **In-memory**                  | `ConcurrentHashMap`        | Great for testing and ephemeral data   |
| **Encryption Layer**           | `AES-GCM`                  | Transparent chunked encryption wrapper |
| **Metrics Layer**              | `Micrometer`               | Optional metrics wrapper               |
| **Observation Layer**          | `Micrometer`               | Optional observation wrapper           |

------

## Maven dependency

```xml
<dependency>
    <groupId>io.github.temesoft</groupId>
    <artifactId>file-storage</artifactId>
    <version>1.9</version>
</dependency>
```

------

## Gradle dependency

```gradle
testImplementation 'io.github.temesoft:file-storage:1.9'
```

-------

## Code examples

#### System file storage initialization
```java
FileStorageService<UUID> fileStorageService = new SystemFileStorageServiceImpl<>(
        UUIDFileStorageId::new, 
        Path.of("/some/root/path")
);
```

#### Amazon AWS S3 file storage initialization
```java
FileStorageService<UUID> fileStorageService = new S3FileStorageServiceImpl<>(
        UUIDFileStorageId::new,
        S3Client.builder()
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
            ))
            .region(Region.of(REGION))
            .build(), 
        BUCKET_NAME
);
```

#### Google cloud storage initialization
```java
FileStorageService<UUID> fileStorageService = new GcsFileStorageServiceImpl<>(
        UUIDFileStorageId::new,
        StorageOptions.newBuilder()
                .setHost("https://storage.googleapis.com")
                .setProjectId("your-project-id")
                .setCredentials(ApiKeyCredentials.create("your-key-here"))
                .build()
                .getService(), 
        BUCKET_NAME
);
```

#### Azure cloud storage initialization
```java
FileStorageService<UUID> fileStorageService = new AzureFileStorageServiceImpl<>(
        UUIDFileStorageId::new,
        new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient()
                .getBlobContainerClient(BUCKET_NAME)
);
```

#### SFTP file storage initialization (Password)
```java
Properties props = new Properties();
props.setProperty("StrictHostKeyChecking", "no");
FileStorageService<UUID> fileStorageService = new SftpFileStorageServiceImpl<>(
        UUIDFileStorageId::new,
        SFTP_HOSTNAME,
        SFTP_PORT,
        USERNAME,
        PASSWORD,
        ROOT_DIRECTORY,
        props
);
```

#### SFTP file storage initialization (SSH Key)
```java
Properties props = new Properties();
props.setProperty("StrictHostKeyChecking", "no");
FileStorageService<UUID> fileStorageService = new SftpFileStorageServiceImpl<>(
        UUIDFileStorageId::new,
        SFTP_HOSTNAME,
        SFTP_PORT,
        USERNAME,
        null, // password
        PRIVATE_KEY_PATH,
        PASSPHRASE,
        ROOT_DIRECTORY,
        props
);
```


#### HDFS file storage initialization
```java
FileStorageService<UUID> fileStorageService = new HdfsFileStorageServiceImpl<>(
        UUIDFileStorageId::new, 
        hdfsFileSystem
);
```

#### In-memory file storage initialization
```java
FileStorageService<UUID> fileStorageService = new InMemoryFileStorageServiceImpl<>(
        UUIDFileStorageId::new
);
```

-------

## Service Wrappers (Layering)
The library uses the Decorator pattern to allow layering additional functionality over any base storage implementation.
This is managed via the [FileStorageServiceWrapper](src/main/java/com/temesoft/fs/FileStorageServiceWrapper.java) interface.

### Slf4j Logging Wrapper
In order to enable logging for file storage services please use this simple wrapper implementation [LoggingFileStorageServiceWrapper](src/main/java/com/temesoft/fs/LoggingFileStorageServiceWrapper.java). 
This class provides `debug` level logging, taking as constructor argument - actual underlying [FileStorageService](src/main/java/com/temesoft/fs/FileStorageService.java)
implementation. Please note that file storage service Spring beans created by [FileStorageBeanRegistryConfiguration](src/main/com/temesoft/fs/spring/FileStorageBeanRegistryConfiguration.java)
are already using this service wrapper.

Example of manual logging wrapper instantiation:
```java
FileStorageService<UUID> fileStorageService = new LoggingFileStorageServiceWrapper<>(
        new InMemoryFileStorageServiceImpl<>(UUIDFileStorageId::new)
);

```
Log output example using service setup from above, for all methods defined in [FileStorageService](src/main/java/com/temesoft/fs/FileStorageService.java):
```
thread=main level=DEBUG exists('0e3d3454-1d4f-42ac-a07a-8df859226119')
thread=main level=DEBUG doesNotExist('0e3d3454-1d4f-42ac-a07a-8df859226119')
thread=main level=DEBUG getSize('0e3d3454-1d4f-42ac-a07a-8df859226119')
thread=main level=DEBUG delete('0e3d3454-1d4f-42ac-a07a-8df859226119')
thread=main level=DEBUG create('0e3d3454-1d4f-42ac-a07a-8df859226119', 1024 bytes)
thread=main level=DEBUG delete('0e3d3454-1d4f-42ac-a07a-8df859226119')
thread=main level=DEBUG create('0e3d3454-1d4f-42ac-a07a-8df859226119', java.io.ByteArrayInputStream@17063c32, 1024)
thread=main level=DEBUG getBytes('0e3d3454-1d4f-42ac-a07a-8df859226119')
thread=main level=DEBUG getBytes('0e3d3454-1d4f-42ac-a07a-8df859226119', 123, 456)
thread=main level=DEBUG getInputStream('0e3d3454-1d4f-42ac-a07a-8df859226119')
thread=main level=DEBUG delete('0e3d3454-1d4f-42ac-a07a-8df859226119')
thread=main level=DEBUG deleteAll()
```

### Transparent Encryption Wrapper
Provides data-at-rest protection using **AES-GCM** with chunked processing via [EncryptingFileStorageServiceWrapper](src/main/java/com/temesoft/fs/EncryptingFileStorageServiceWrapper.java).
Files are encrypted in discrete chunks, allowing for efficient random-access reads (range requests) without decrypting the entire file.

```java
FileStorageCryptor cryptor = new AesGcmChunkedFileStorageCryptor(
        "my-key-id", 
        "base64-encoded-256-bit-aes-key"
);

FileStorageService<UUID> encryptedStorage = new EncryptingFileStorageServiceWrapper<>(
        new SystemFileStorageServiceImpl<>(UUIDFileStorageId::new, Path.of("/data")),
        cryptor,
        65536 // 64KB chunk size
);
```

#### Encrypted File Layout:
- **Header:** Metadata including version, algorithm, chunk size, original size, and key ID.
- **Chunks:** Each chunk is stored as `[12-byte Nonce][Ciphertext][16-byte GCM Tag]`.


### Metrics Wrapper
A decorator that records operational metrics using Micrometer. It captures execution time and frequency for all storage operations.

```java
FileStorageService<UUID> metricsStorage = new MetricsFileStorageServiceWrapper<>(
        new InMemoryFileStorageServiceImpl<>(UUIDFileStorageId::new)
);
// setRegistry(...) method is annotated with @Autowired in case metricsStorage will be a bean
metricsStorage.setRegistry(meterRegistry);
// Metrics will be available under 'file.storage.<implementation-name>'
```

### Observation Wrapper
Provides comprehensive observability using Micrometer Observation API, including metrics, distributed tracing 
(e.g., Brave, OpenTelemetry, etc.), and correlated logging.

```java
FileStorageService<UUID> observedStorage = new ObservationFileStorageServiceWrapper<>(
        new InMemoryFileStorageServiceImpl<>(UUIDFileStorageId::new)
);
// setObservationRegistry(...) method is annotated with @Autowired in case metricsStorage will be a bean
observedStorage.setObservationRegistry(observationRegistry);
```

### User Defined Wrapper
```java
// User defined wrapper needs to implement FileStorageServiceWrapper 
// and have a constructor taking FileStorageService
public class CustomFileStorageServiceWrapper<T> implements FileStorageServiceWrapper<T> {

    private final FileStorageService<T> delegate;

    public CustomFileStorageServiceWrapper(final FileStorageService<T> delegate) {
        this.delegate = delegate;
    }
    // TODO: Your custom code goes here...
}
```

-------

## Spring-boot usage

File storage service beans can be configured using a simple property interface by using 
[FileStorageBeanRegistryConfiguration](src/main/java/com/temesoft/fs/spring/FileStorageBeanRegistryConfiguration.java).
Following is a list of possible configuration properties:
```properties
# Global setting for actuator endpoint
app.file-storage.endpoint-enabled=true

# Instance configuration
app.file-storage.instances.widget-mem.type=InMemory
app.file-storage.instances.widget-mem.bean-qualifier=widgetFileStorage
app.file-storage.instances.widget-mem.entity-class=org.some.where.Widget
# idService should implement com.temesoft.fs.FileStorageIdService<Widget>
app.file-storage.instances.widget-mem.id-service=org.some.where.WidgetFileStorageIdService
# Additional custom wrappers can be added. 
# They must implement FileStorageServiceWrapper and have a constructor taking FileStorageService
app.file-storage.instances.widget-mem.custom-wrappers=\
  com.temesoft.fs.spring.MetricsFileStorageServiceWrapper,\
  com.temesoft.fs.spring.ObservationFileStorageServiceWrapper,\
  com.company.YourCustomFileStorageServiceWrapper

# Instance with Encryption enabled
app.file-storage.instances.secure-sys.type=System
app.file-storage.instances.secure-sys.bean-qualifier=secureFileStorage
app.file-storage.instances.secure-sys.entity-class=org.some.where.Document
app.file-storage.instances.secure-sys.id-service=org.some.where.DocumentIdService
app.file-storage.instances.secure-sys.system.root-location=/tmp/secure-data
# Encryption settings
app.file-storage.instances.secure-sys.encryption.enabled=true
app.file-storage.instances.secure-sys.encryption.key-id=prod-key-1
app.file-storage.instances.secure-sys.encryption.base64-key=MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=
app.file-storage.instances.secure-sys.encryption.chunk-size=65536

app.file-storage.instances.trinket-sys.type=System
app.file-storage.instances.trinket-sys.bean-qualifier=trinketSysFileStorage
app.file-storage.instances.trinket-sys.entity-class=org.some.where.Trinket
app.file-storage.instances.trinket-sys.id-service=org.some.where.TrinketFileStorageIdService
app.file-storage.instances.trinket-sys.system.root-location=/tmp/test-file-storage

app.file-storage.instances.trinket-sftp.type=Sftp
app.file-storage.instances.trinket-sftp.bean-qualifier=trinketSftpFileStorage
app.file-storage.instances.trinket-sftp.entity-class=org.some.where.Trinket
app.file-storage.instances.trinket-sftp.id-service=org.some.where.TrinketFileStorageIdService
app.file-storage.instances.trinket-sftp.sftp.remote-host=127.0.0.1
app.file-storage.instances.trinket-sftp.sftp.remote-port=12345
app.file-storage.instances.trinket-sftp.sftp.username=username
# Either password or private-key-path is required
app.file-storage.instances.trinket-sftp.sftp.password=password
app.file-storage.instances.trinket-sftp.sftp.private-key-path=/path/to/id_rsa
app.file-storage.instances.trinket-sftp.sftp.passphrase=optional_passphrase
app.file-storage.instances.trinket-sftp.sftp.root-directory=/tmp/test-file-storage
# Additional configuration for jsch sftp (for example "StrictHostKeyChecking=no")
app.file-storage.instances.trinket-sftp.sftp.config-properties.StrictHostKeyChecking=no

# S3 integration will require a bean software.amazon.awssdk.services.s3.S3Client
app.file-storage.instances.trinket-s3.type=S3
app.file-storage.instances.trinket-s3.entity-class=org.some.where.Trinket
app.file-storage.instances.trinket-s3.id-service=org.some.where.TrinketFileStorageIdService
app.file-storage.instances.trinket-s3.bean-qualifier=trinketS3FileStorage
app.file-storage.instances.trinket-s3.s3.bucket-name=test-bucket

# GCS integration will require a bean com.google.cloud.storage.Storage
app.file-storage.instances.trinket-gcs.type=GCS
app.file-storage.instances.trinket-gcs.entity-class=org.some.where.Trinket
app.file-storage.instances.trinket-gcs.id-service=org.some.where.TrinketFileStorageIdService
app.file-storage.instances.trinket-gcs.bean-qualifier=trinketGcsFileStorage
app.file-storage.instances.trinket-gcs.gcs.bucket-name=test-bucket

# Azure integration will require a bean com.azure.storage.blob.BlobContainerClient
app.file-storage.instances.trinket-azure.type=Azure
app.file-storage.instances.trinket-azure.bean-qualifier=trinketAzureFileStorage
app.file-storage.instances.trinket-azure.entity-class=org.some.where.Trinket
app.file-storage.instances.trinket-azure.id-service=org.some.where.TrinketFileStorageIdService
app.file-storage.instances.trinket-azure.azure.bucket-name=test-bucket

# HDFS integration will require a bean org.apache.hadoop.fs.FileSystem
app.file-storage.instances.trinket-hdfs.type=HDFS
app.file-storage.instances.trinket-hdfs.bean-qualifier=trinketHdfsFileStorage
app.file-storage.instances.trinket-hdfs.entity-class=org.some.where.Trinket
app.file-storage.instances.trinket-hdfs.id-service=org.some.where.TrinketFileStorageIdService
app.file-storage.instances.trinket-hdfs.azure.bucket-name=test-bucket
```

#### Actuator Endpoint
When enabled, the `/actuator/file-storage` endpoint provides a JSON view of all registered storage beans, their descriptions, and underlying implementations (unwrapping any decorators).

## Custom storage id usage

Let's imagine that we have an entity `BookEntity` and it has fields like: `publishYear`, `publishMonth`, and `isbnNumber`
and we will use those fields to create storage path.

```java
public class BookEntityFileStorageId extends FileStorageId<BookEntity> {

    public BookEntityFileStorageId(BookEntity bookEntity) {
      super(bookEntity);
    }
  
    /**
     * Returns relative path generated from provided entity attributes
     */
    @Override
    public String generatePath() {
      return bookEntity.getPublishYear() + 
              "/" + bookEntity.getPublishMonth() + 
              "/" + bookEntity.getIsbnNumber();
    }
}

FileStorageService<BookEntity> fileStorageService = new InMemoryFileStorageServiceImpl<>(BookEntityFileStorageId::new);
```

-------

## Storage service usage
Let's use provided [UUIDFileStorageId](src/main/java/com/temesoft/fs/UUIDFileStorageId.java) and
[InMemoryFileStorageServiceImpl](src/main/java/com/temesoft/fs/InMemoryFileStorageServiceImpl.java) for this example.
Other implementations are also available in addition to public interface to implement custom ID / storage path setup.

```java
// Setup file storage service using in-memory implementation with ksuid type of storage id
FileStorageService<UUID> fileStorageService = new InMemoryFileStorageServiceImpl<>(UUIDFileStorageId::new);

// Set storageId to some value
UUID storageId = UUID.fromString("32d18211-9fc4-4876-ac9d-33a6b150205a");

// Checks by id if file exists, and if it does - returns true
boolean fileExists = fileStorageService.exists(storageId);

// Checks by id if file does not exist, and if it does - returns false
boolean fileDoesNotExist = fileStorageService.doesNotExist(storageId);

// Returns size of file content in bytes using provided id
long size = fileStorageService.getSize(storageId);

// Creates file using provided id and byte array
fileStorageService.create(storageId, byteArrayToStore);

// Creates file using provided id and byte array, when overwrite is TRUE, tries to delete the file before creation
fileStorageService.create(storageId, byteArrayToStore, overwrite);

// Creates file using provided id and input stream
fileStorageService.create(storageId, inputStreamWithContentToStore, contentSize);

// Creates file using provided id and input stream, when overwrite is TRUE, tries to delete the file before creation
fileStorageService.create(storageId, inputStreamWithContentToStore, contentSize, overwrite);

// Returns byte array of file content using provided id
byte[] storedContent = fileStorageService.getBytes(storageId);

// Returns byte array range of file content using provided id, startPosition and endPosition
byte[] storedContentRange = fileStorageService.getBytes(storageId, startPosition, endPosition);

// Returns input stream of file content using provided id
InputStream storedContentInputStream = fileStorageService.getInputStream(storageId);

// Deletes file using provided id
fileStorageService.delete(storageId);

// Deletes file if it exists and if it does not exist simply returns
fileStorageService.deleteIfExists(storageId);

// Deletes all available files
fileStorageService.deleteAll();
```

-------

## Error Handling
The library provides a unified exception model using a single unchecked exception: 
[FileStorageException](src/main/java/com/temesoft/fs/FileStorageException.java). 

All implementations catch backend-specific exceptions (e.g., `S3Exception`, `SftpException`, `IOException`) 
and wrap them into a `FileStorageException` with a descriptive message and the original cause.

Common failure scenarios that throw `FileStorageException`:
- **File Not Found:** Attempting to delete or read a non-existent file.
- **Connection Failure:** Network issues with cloud providers or SFTP servers.
- **Authentication Failure:** Incorrect credentials provided for storage backends.
- **Access Denied:** Insufficient permissions to perform a requested storage operation.
- **File Already Exists:** Attempting to create a file that already exists without the `overwrite` flag.
- **Range Error:** Invalid `startPosition` or `endPosition` for partial content reads.

Example of handling exceptions:
```java
try {
    fileStorageService.create(id, bytes);
} catch (FileStorageException e) {
    // Handle the storage error (e.g., log, retry, or fallback)
    LOGGER.error("Storage action failed: {}", e.getMessage(), e.getCause());
}
```

-------

## Support & contribution
To contribute to this project - please create a PR :smiley:

-------

## License
[GNU General Public License v3](https://www.gnu.org/licenses/quick-guide-gplv3.html)

* the freedom to use the software for any purpose,
* the freedom to change the software to suit your needs,
* the freedom to share the software with your friends and neighbors
* the freedom to share the changes you make.