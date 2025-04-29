[![Java CI](https://github.com/temesoft/file-storage/actions/workflows/main.yml/badge.svg)](https://github.com/temesoft/file-storage/actions/workflows/main.yml)
[![Javadoc](https://javadoc.io/badge2/io.github.temesoft/file-storage/javadoc.svg)](https://javadoc.io/doc/io.github.temesoft/file-storage)
<img src='https://raw.githubusercontent.com/temesoft/file-storage/refs/heads/main/jacoco.svg' alt='Test coverage (jacoco)' title='Test coverage (jacoco)'>

# file-storage #

A file storage library with a simple common interface, flexible IDs, and custom path generation

### Provided file storage implementations include:

- File system storage (using `java.nio`)
- Amazon AWS S3 storage (AWS SDK v2.x)
- Google cloud storage (GCS SDK v2.x)
- Azure cloud storage (azure-storage-blob v12.x)
- SFTP storage (jsch v0.2.x)
- HDFS storage (hadoop v3.x)
- In-memory storage (using `java.util.concurrent.ConcurrentHashMap`)
- Spring-Boot integration (spring-boot 3.x)

------

### Features include:
- Custom path generator which can be based on your specific id or entity attribute(s)
- Ability to store and retrieve using `java.io.InputStream` or `byte[]`
- Ability to retrieve range of file (for possible streaming / HTTP 206 partial content)
- Spring-boot simple property configuration integration
- Easy integration interfaces:
  - [FileStorageService.java](src/main/java/com/temesoft/fs/FileStorageService.java)
  - [FileStorageId.java](src/main/java/com/temesoft/fs/FileStorageId.java)
- File storage id providers include: 
  - [UUID](src/main/java/com/temesoft/fs/UUIDFileStorageId.java)
  - [Ksuid](src/main/java/com/temesoft/fs/KsuidFileStorageId.java)

-------

## Maven dependency

Add the dependency to maven pom.xml:

```xml
<dependency>
    <groupId>io.github.temesoft</groupId>
    <artifactId>file-storage</artifactId>
    <version>1.6</version>
</dependency>
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

#### SFTP file storage initialization
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

## Slf4j Logging (service wrapper)
In order to enable logging for file storage services please use this simple wrapper implementation [LoggingFileStorageServiceWrapper.java](src/main/java/com/temesoft/fs/LoggingFileStorageServiceWrapper.java). 
This class provides `debug` level logging, taking as constructor argument - actual underlying [FileStorageService.java](src/main/java/com/temesoft/fs/FileStorageService.java)
implementation. Please note that file storage service Spring beans created by [FileStorageBeanRegistryConfiguration.java](src/main/com/temesoft/fs/spring/FileStorageBeanRegistryConfiguration.java)
are already using this service wrapper.

Example of manual instantiation:
```java
FileStorageService<UUID> fileStorageService = new LoggingFileStorageServiceWrapper<>(
        new InMemoryFileStorageServiceImpl<>(UUIDFileStorageId::new)
);


```

-------

## Spring-boot usage

File storage service beans can be configured using a simple property interface by using 
[FileStorageBeanFactoryConfiguration.java](src/main/java/com/temesoft/fs/spring/FileStorageBeanFactoryConfiguration.java).
Following is a list of possible configuration properties to be used with provided bean factory configuration:
```properties
app.file-storage.instances.instances.widget-mem.type=InMemory
app.file-storage.instances.instances.widget-mem.bean-qualifier=widgetFileStorage
app.file-storage.instances.instances.widget-mem.entity-class=org.some.where.Widget
# idService should implement com.temesoft.fs.FileStorageIdService<Widget>
app.file-storage.instances.instances.widget-mem.id-service=org.some.where.WidgetFileStorageIdService

app.file-storage.instances.instances.trinket-mem.type=InMemory
app.file-storage.instances.instances.trinket-mem.bean-qualifier=trinketFileStorage
app.file-storage.instances.instances.trinket-mem.entity-class=org.some.where.Trinket
app.file-storage.instances.instances.trinket-mem.id-service=org.some.where.TrinketFileStorageIdService

app.file-storage.instances.instances.trinket-sys.type=System
app.file-storage.instances.instances.trinket-sys.bean-qualifier=trinketSysFileStorage
app.file-storage.instances.instances.trinket-sys.entity-class=org.some.where.Trinket
app.file-storage.instances.trinket-sys.id-service=org.some.where.TrinketFileStorageIdService
app.file-storage.instances.trinket-sys.system.root-location=/tmp/test-file-storage

app.file-storage.instances.trinket-sftp.type=Sftp
app.file-storage.instances.trinket-sftp.bean-qualifier=trinketSftpFileStorage
app.file-storage.instances.trinket-sftp.entity-class=org.some.where.Trinket
app.file-storage.instances.trinket-sftp.id-service=org.some.where.TrinketFileStorageIdService
app.file-storage.instances.trinket-sftp.sftp.remote-host=127.0.0.1
app.file-storage.instances.trinket-sftp.sftp.remote-port=12345
app.file-storage.instances.trinket-sftp.sftp.username=username
app.file-storage.instances.trinket-sftp.sftp.password=password
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


-------

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

// Creates file using provided id and input stream
fileStorageService.create(storageId, inputStreamWithContentToStore, contentSize);

// Returns byte array of file content using provided id
byte[] storedContent = fileStorageService.getBytes(storageId);

// Returns byte array range of file content using provided id, startPosition and endPosition
byte[] storedContentRange = fileStorageService.getBytes(storageId, startPosition, endPosition);

// Returns input stream of file content using provided id
InputStream storedContentInputStream = fileStorageService.getInputStream(storageId);

// Deletes file using provided id
fileStorageService.delete(storageId);

// Deletes all available files
fileStorageService.deleteAll();
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