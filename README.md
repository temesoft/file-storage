[![Java CI](https://github.com/temesoft/file-storage/actions/workflows/main.yml/badge.svg)](https://github.com/temesoft/file-storage/actions/workflows/main.yml)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.temesoft/file-storage/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.temesoft/file-storage)
[![Javadoc](https://javadoc.io/badge2/io.github.temesoft/file-storage/javadoc.svg)](https://javadoc.io/doc/io.github.temesoft/file-storage)


# file-storage #

A file storage library with a simple common interface, flexible IDs, and custom path generation

### Provided file storage implementations include:

- File system storage (using `java.nio`)
- AWS S3 storage (AWS SDK v2.x)
- SFTP storage (jsch v0.2.x)
- HDFS storage (hadoop v3.x)
- In-memory storage (using `java.util.concurrent.ConcurrentHashMap`)

------

### Features include:
- Custom path generator which can be based on your specific id or entity attribute(s)
- Ability to store and retrieve using `java.io.InputStream` or `byte[]`
- Ability to retrieve range of file (for possible streaming / HTTP 206 partial content)
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
    <version>1.3</version>
</dependency>
```

-------

## Service initialization

#### System file storage initialization
```java
FileStorageService fileStorageService = new SystemFileStorageServiceImpl("/some/root/path");
```

#### AWS S3 file storage initialization
```java
FileStorageService fileStorageService = new S3FileStorageServiceImpl(
        S3Client.builder()
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
            ))
            .region(Region.of(REGION))
            .build(), 
        BUCKET_NAME
);
```

#### SFTP file storage initialization
```java
Properties props = new Properties();
props.setProperty("StrictHostKeyChecking", "no");
FileStorageService fileStorageService = new SftpFileStorageServiceImpl(
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
FileStorageService fileStorageService = new HdfsFileStorageServiceImpl(hdfsFileSystem);
```

#### In-memory file storage initialization
```java
FileStorageService fileStorageService = new InMemoryFileStorageServiceImpl();
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
```

-------

## Storage service usage
Let's use provided [UUIDFileStorageId](src/main/java/com/temesoft/fs/UUIDFileStorageId.java) and
[InMemoryFileStorageServiceImpl](src/main/java/com/temesoft/fs/InMemoryFileStorageServiceImpl.java) for this example.
Other implementations are also available in addition to public interface to implement custom ID / storage path setup.
```java
FileStorageService fileStorageService = new InMemoryFileStorageServiceImpl();

FileStorageId storageId = new UUIDFileStorageId(someUuid);

// Checks by id if file exists, and if it does - returns true
boolean fileExists = fileStorageService.exists(storageId);

// Checks by id if file does not exist, and if it does - returns false
boolean fileDoesNotExist = fileStorageService.doesNotExist(storageId);

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