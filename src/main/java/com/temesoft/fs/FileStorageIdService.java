package com.temesoft.fs;

/**
 * Interface for conversion of typed parameter "T" into {@link FileStorageId} for use in {@link FileStorageService}
 */
@FunctionalInterface
public interface FileStorageIdService<T> {

    FileStorageId<T> fromId(T value);

}
