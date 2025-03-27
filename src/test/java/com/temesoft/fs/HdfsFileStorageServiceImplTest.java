package com.temesoft.fs;

import com.github.ksuid.Ksuid;
import com.google.common.io.Files;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
    private static FileStorageService fileStorageService;

    @BeforeAll
    public static void setUp() throws IOException {
        final Configuration conf = new Configuration();
        conf.set("dfs.permissions.enabled", "false");
        final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf, Files.createTempDir())
                .manageNameDfsDirs(true)
                .manageDataDfsDirs(true)
                .format(true)
                .build();
        cluster.waitActive();
        hdfs = FileSystem.get(conf);
        fileStorageService = new HdfsFileStorageServiceImpl(hdfs);
    }

    @AfterAll
    public static void tearDown() throws IOException {
        hdfs.close();
    }

    @Test
    public void testHdfsFileStorageService() throws IOException {
        assertThatNoException().isThrownBy(() -> fileStorageService.create(STORAGE_FILE_ID, BYTE_CONTENT));
        assertThat(fileStorageService.getBytes(STORAGE_FILE_ID)).isEqualTo(BYTE_CONTENT);
        assertThat(fileStorageService.getBytes(STORAGE_FILE_ID, 10, 20))
                .isEqualTo(Arrays.copyOfRange(BYTE_CONTENT, 10, 20));
        assertThat(IOUtils.toByteArray(fileStorageService.getInputStream(STORAGE_FILE_ID))).isEqualTo(BYTE_CONTENT);

        assertThatThrownBy(() -> fileStorageService.create(STORAGE_FILE_ID, BYTE_CONTENT))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to create file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File already exist");

        assertThatNoException().isThrownBy(() -> fileStorageService.delete(STORAGE_FILE_ID));
        assertThat(fileStorageService.getStorageDescription()).isNotEmpty();

        assertThatNoException().isThrownBy(() -> fileStorageService.
                create(STORAGE_FILE_ID, new ByteArrayInputStream(BYTE_CONTENT), BYTE_CONTENT.length));

        assertThatNoException().isThrownBy(fileStorageService::deleteAll);
        assertThatThrownBy(() -> fileStorageService.getBytes(STORAGE_FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes from file with ID: %s", FILE_ID);
    }

    @Test
    public void testHdfsFileStorageService_Exceptions() {
        assertThatThrownBy(() -> fileStorageService.getBytes(STORAGE_FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes from file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found: %s", STORAGE_FILE_ID.generatePath());

        assertThatThrownBy(() -> fileStorageService.getInputStream(STORAGE_FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get input stream from file with ID: %s", FILE_ID);

        assertThatThrownBy(() -> fileStorageService.getBytes(STORAGE_FILE_ID, 10, 20))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to get bytes range from file with ID: %s", FILE_ID);

        assertThatThrownBy(() -> fileStorageService.delete(STORAGE_FILE_ID))
                .isInstanceOf(FileStorageException.class)
                .hasMessage("Unable to delete file with ID: %s", FILE_ID)
                .hasRootCauseMessage("File not found: %s", STORAGE_FILE_ID.generatePath());
    }
}