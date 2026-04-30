package com.temesoft.fs.spring;

import com.temesoft.fs.FileStorageException;
import com.temesoft.fs.FileStorageIdService;
import com.temesoft.fs.FileStorageService;
import com.temesoft.fs.FileStorageServiceWrapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.InputStream;
import java.util.Locale;

/**
 * A decorator for {@link FileStorageService} that records operational metrics using the
 * Micrometer {@link MeterRegistry}.
 *
 * <p>This wrapper captures the execution time and frequency of all storage operations
 * (e.g., create, delete, exists) using {@link Timer} samples. Metrics are dynamically
 * named based on the underlying storage implementation's description.
 *
 * <p><strong>Metric Naming &amp; Tagging:</strong>
 * <ul>
 *     <li><strong>Metric Name:</strong> {@code file.storage.<description>} (where spaces in
 *         the description are replaced with dashes).</li>
 *     <li><strong>Tags:</strong> Each metric includes an {@code operation} tag identifying
 *          the specific method called (e.g., {@code create_bytes}, {@code get_size}).</li>
 * </ul>
 *
 * <p><strong>Dependency Injection:</strong>
 * This class is designed to be Spring-aware. While it is instantiated with a
 * {@link FileStorageService} delegate via the constructor, the {@link MeterRegistry}
 * is injected via setter injection ({@link #setRegistry(MeterRegistry)}), typically
 * triggered by a post-instantiation autowiring process.
 *
 * @param <T> The type used for file identifiers in the storage service.
 * @see FileStorageServiceWrapper
 * @see MeterRegistry
 * @see Timer
 */
public class MetricsFileStorageServiceWrapper<T> implements FileStorageServiceWrapper<T> {

    private final FileStorageService<T> delegate;
    private final String metricPrefix;

    private MeterRegistry registry;

    public MetricsFileStorageServiceWrapper(final FileStorageService<T> delegate) {
        this.delegate = delegate;
        this.metricPrefix = "file.storage." + FileStorageServiceWrapper.unwrap(delegate).getStorageDescription()
                .toLowerCase(Locale.ROOT).replace(" ", "-");
    }

    @Override
    public FileStorageService<T> getService() {
        return delegate;
    }

    @Override
    public boolean exists(final T id) throws FileStorageException {
        return record(() -> delegate.exists(id), "exists");
    }

    @Override
    public long getSize(final T id) throws FileStorageException {
        return record(() -> delegate.getSize(id), "get_size");
    }

    @Override
    public void create(final T id, byte[] bytes) throws FileStorageException {
        recordVoid(() -> delegate.create(id, bytes), "create_bytes");
    }

    @Override
    public void create(final T id, InputStream inputStream, long contentSize) throws FileStorageException {
        recordVoid(() -> delegate.create(id, inputStream, contentSize), "create_stream");
    }

    @Override
    public void delete(final T id) throws FileStorageException {
        recordVoid(() -> delegate.delete(id), "delete");
    }

    @Override
    public byte[] getBytes(final T id) throws FileStorageException {
        return record(() -> delegate.getBytes(id), "get_bytes");
    }

    @Override
    public byte[] getBytes(final T id, final long start, final long end) throws FileStorageException {
        return record(() -> delegate.getBytes(id, start, end), "get_bytes_range");
    }

    @Override
    public InputStream getInputStream(final T id) throws FileStorageException {
        return record(() -> delegate.getInputStream(id), "get_input_stream");
    }

    @Override
    public void deleteAll() throws FileStorageException {
        recordVoid(delegate::deleteAll, "delete_all");
    }

    @Override
    public FileStorageIdService<T> getFileStorageIdService() {
        return delegate.getFileStorageIdService();
    }

    /**
     * Describes the wrapper and storage type of implementation
     */
    @Override
    public String getStorageDescription() {
        return "Metrics(" + delegate.getStorageDescription() + ")";
    }

    private <R> R record(final StorageSupplier<R> supplier, final String operation) throws FileStorageException {
        final Timer.Sample sample = Timer.start(registry);
        try {
            return supplier.get();
        } finally {
            sample.stop(Timer.builder(metricPrefix)
                    .tag("operation", operation)
                    .description("Execution time for " + operation)
                    .register(registry));
        }
    }

    private void recordVoid(final StorageRunnable runnable, final String operation) throws FileStorageException {
        final Timer.Sample sample = Timer.start(registry);
        try {
            runnable.run();
        } finally {
            sample.stop(Timer.builder(metricPrefix)
                    .tag("operation", operation)
                    .register(registry));
        }
    }

    @FunctionalInterface
    private interface StorageSupplier<R> {
        R get() throws FileStorageException;
    }

    @FunctionalInterface
    private interface StorageRunnable {
        void run() throws FileStorageException;
    }

    @Autowired
    public void setRegistry(final MeterRegistry registry) {
        this.registry = registry;
    }
}