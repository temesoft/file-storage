package com.temesoft.fs;

/**
 * Abstract class for standard path generation from provided ID for use with {@link FileStorageService}
 */
public abstract class FileStorageId<T> {

    public static final String SEPARATOR = "/";

    private final T value;

    /**
     * Base constructor for {@link FileStorageId} taking typed parameter as argument
     */
    public FileStorageId(final T value) {
        this.value = value;
    }

    /**
     * Method returns a relative path generated from provided ID
     */
    public abstract String generatePath();

    /**
     * Common path generation logic based on first 4 characters
     */
    protected String generateStandardPath(final String idString) {
        return idString.charAt(0)
                + SEPARATOR + idString.charAt(1)
                + SEPARATOR + idString.charAt(2)
                + SEPARATOR + idString.charAt(3)
                + SEPARATOR + idString.substring(4);
    }

    /**
     * Returns actual value for provided ID
     */
    public T value() {
        return value;
    }

    @Override
    public String toString() {
        return generatePath();
    }
}