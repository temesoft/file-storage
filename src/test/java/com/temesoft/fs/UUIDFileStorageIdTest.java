package com.temesoft.fs;


import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class UUIDFileStorageIdTest {

    @Test
    public void testGeneratePath() {
        final UUID id = UUID.fromString("467f28f8-5a5a-4f10-9fce-ed2b5eb5ddd4");
        final var storageId = new UUIDFileStorageId(id);
        assertThat(id).isNotNull();
        assertThat(storageId.generatePath()).isEqualTo("4/6/7/f/28f8-5a5a-4f10-9fce-ed2b5eb5ddd4");
        assertThat(storageId.value()).isEqualTo(id);
    }
}