package com.temesoft.fs;

import java.util.ArrayList;
import java.util.List;

/**
 * A decorator interface for {@link FileStorageService} that allows for layering additional
 * functionality (such as encryption, logging, or caching) over a base storage implementation.
 * This follows the Decorator pattern, where each wrapper holds a reference to the underlying
 * service it is enhancing.
 *
 * @param <T> The type of file identifier used by the service.
 */
public interface FileStorageServiceWrapper<T> extends FileStorageService<T> {

    /**
     * Retrieves the immediate underlying service being decorated by this wrapper.
     *
     * @return The next {@link FileStorageService} in the decoration chain.
     */
    FileStorageService<T> getService();

    @Override
    default String getStorageDescription() {
        return getService().getStorageDescription();
    }

    /**
     * Unwraps a potentially decorated service to find the base implementation at the
     * bottom of the stack.
     * This method traverses the chain of {@link FileStorageServiceWrapper} instances
     * until it reaches a service that is not a wrapper.
     *
     * @param service The service to unwrap.
     * @return The base {@link FileStorageService} implementation.
     */
    static FileStorageService<?> unwrap(final FileStorageService<?> service) {
        FileStorageService<?> current = service;
        while (current instanceof FileStorageServiceWrapper<?> wrapper) {
            current = wrapper.getService();
        }
        return current;
    }

    /**
     * Traverses the decoration chain of a {@link FileStorageService} and returns a list
     * of all applied wrapper class names.
     * This method starts from the outermost wrapper and moves down the stack until it
     * reaches the base implementation. Each encountered wrapper that implements
     * {@link FileStorageServiceWrapper} has its fully qualified class name added to the list.
     *
     * @param service The potentially decorated service to inspect.
     * @return An ordered {@link List} of class names representing the wrapper stack,
     * ordered from outermost to innermost.
     */
    static List<String> listWrappers(final FileStorageService<?> service) {
        final List<String> result = new ArrayList<>();
        FileStorageService<?> current = service;
        while (current instanceof FileStorageServiceWrapper<?> wrapper) {
            result.add(wrapper.getClass().getName());
            current = wrapper.getService();
        }
        return result;
    }
}
