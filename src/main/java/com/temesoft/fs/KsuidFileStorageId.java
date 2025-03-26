package com.temesoft.fs;

import com.github.ksuid.Ksuid;

/**
 * Ksuid file storage ID provider
 * Example: ID "1HCpXwx2EK9oYluWbacgeCnFcLf" will become a path "1/H/C/p/Xwx2EK9oYluWbacgeCnFcLf"
 * <a href="https://github.com/ksuid/ksuid">Ksuid</a>
 */
public class KsuidFileStorageId extends FileStorageId<Ksuid> {

    /**
     * Constructor taking {@link Ksuid} as argument
     */
    public KsuidFileStorageId(final Ksuid value) {
        super(value);
    }

    /**
     * Method returns a relative path generated from provided ID
     */
    @Override
    public String generatePath() {
        final String idString = value().toString();
        return idString.charAt(0)
               + SEPARATOR + idString.charAt(1)
               + SEPARATOR + idString.charAt(2)
               + SEPARATOR + idString.charAt(3)
               + SEPARATOR + idString.substring(4);
    }
}