package com.temesoft.fs;

import com.github.ksuid.Ksuid;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.secure;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HdfsFileStorageServiceImplTest {

    private static final byte[] BYTE_CONTENT = secure().nextAlphanumeric(128).getBytes(UTF_8);
    private static final Ksuid FILE_ID = Ksuid.newKsuid();
    private static final KsuidFileStorageId STORAGE_FILE_ID = new KsuidFileStorageId(FILE_ID);

    private static FileSystem hdfs;
    private static FileStorageService<Ksuid> fileStorageService;

    @BeforeAll
    public static void setUp() throws IOException {
        final Configuration conf = new Configuration();
        conf.set("dfs.permissions.enabled", "false");
        final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf, Files.createTempDirectory("mini-dfs-cluster").toFile())
                .manageNameDfsDirs(true)
                .manageDataDfsDirs(true)
                .format(true)
                .build();
        cluster.waitActive();
        hdfs = FileSystem.get(conf);
        fileStorageService = new HdfsFileStorageServiceImpl<>(KsuidFileStorageId::new, hdfs);
    }

    @AfterAll
    public static void tearDown() throws IOException {
        hdfs.close();
    }

    @Test
    public void testFileStorageService() throws IOException {
        assertThatNoException().isThrownBy(() -> fileStorageService.create(FILE_ID, BYTE_CONTENT));
        assertThat(fileStorageService.getBytes(FILE_ID)).isEqualTo(BYTE_CONTENT);
        assertThat(fileStorageService.getSize(FILE_ID)).isEqualTo(BYTE_CONTENT.length);
        assertThat(fileStorageService.getBytes(FILE_ID, 10, 20))
                .isEqualTo(Arrays.copyOfRange(BYTE_CONTENT, 10, 20));
        assertThat(IOUtils.toByteArray(fileStorageService.getInputStream(FILE_ID))).isEqualTo(BYTE_CONTENT);

        assertThatThrownBy(() -> fileStorageService.create(FILE_ID, BYTE_CONTENT))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to create file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File already exist");

        assertThatThrownBy(() -> fileStorageService.create(FILE_ID, new ByteArrayInputStream(BYTE_CONTENT), BYTE_CONTENT.length))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to create file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File already exist");

        assertThatNoException().isThrownBy(() -> fileStorageService.delete(FILE_ID));
        assertThat(fileStorageService.getStorageDescription()).isNotEmpty();

        assertThatNoException().isThrownBy(() -> fileStorageService.
                create(FILE_ID, new ByteArrayInputStream(BYTE_CONTENT), BYTE_CONTENT.length));

        assertThatNoException().isThrownBy(fileStorageService::deleteAll);
        assertThatThrownBy(() -> fileStorageService.getBytes(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes from file with ID: %s", FILE_ID);

        assertThatThrownBy(() -> fileStorageService.getSize(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get file size with ID: %s", FILE_ID);
    }

    @Test
    public void testFileStorageService_Exceptions() {
        assertThatThrownBy(() -> fileStorageService.getBytes(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found: %s", STORAGE_FILE_ID.generatePath());

        assertThatThrownBy(() -> fileStorageService.getInputStream(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get input stream from file with ID: %s", FILE_ID);

        assertThatThrownBy(() -> fileStorageService.getBytes(FILE_ID, 10, 20))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes range from file with ID: %s", FILE_ID);

        assertThatThrownBy(() -> fileStorageService.delete(FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to delete file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found: %s", STORAGE_FILE_ID.generatePath());
    }
}