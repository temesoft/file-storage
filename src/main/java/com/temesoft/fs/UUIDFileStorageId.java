package com.temesoft.fs;

import java.util.UUID;

/**
 * UUID file storage ID provider
 * Example: ID "467f28f8-5a5a-4f10-9fce-ed2b5eb5ddd4" will become a path "4/6/7/f/28f8-5a5a-4f10-9fce-ed2b5eb5ddd4"
 * <a href="https://en.wikipedia.org/wiki/Universally_unique_identifier">UUID</a>
 */
public class UUIDFileStorageId extends FileStorageId<UUID> {

    /**
     * Constructor taking {@link UUID} as argument
     */
    public UUIDFileStorageId(final UUID value) {
        super(value);
    }

    /**
     * Method returns a relative path generated from provided ID
     */
    @Override
    public String generatePath() {
        return generateStandardPath(value().toString());
    }
}