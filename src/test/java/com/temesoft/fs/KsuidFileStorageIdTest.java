package com.temesoft.fs;

import com.github.ksuid.Ksuid;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KsuidFileStorageIdTest {

    @Test
    public void testGeneratePath() {
        final Ksuid id = Ksuid.fromString("1HCpXwx2EK9oYluWbacgeCnFcLf");
        final FileStorageId<Ksuid> storageId = new KsuidFileStorageId(id);
        assertThat(storageId).isNotNull();
        final String path = "1/H/C/p/Xwx2EK9oYluWbacgeCnFcLf";
        assertThat(storageId.generatePath()).isEqualTo(path);
        assertThat(storageId.value()).isEqualTo(id);
        assertThat(storageId.toString()).isEqualTo(path);
    }
}