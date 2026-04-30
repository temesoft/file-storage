package com.temesoft.fs.spring;

import com.temesoft.fs.FileStorageException;
import com.temesoft.fs.FileStorageIdService;
import com.temesoft.fs.FileStorageService;
import com.temesoft.fs.FileStorageServiceWrapper;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.InputStream;
import java.util.Locale;

/**
 * A decorator for {@link FileStorageService} that provides comprehensive observability
 * using the Micrometer Observation API.
 *
 * <p>This wrapper enables "three-pillar" observability for storage operations:
 * <ul>
 *     <li><strong>Metrics:</strong> Automatically creates timers and counters for every operation.</li>
 *     <li><strong>Tracing:</strong> Creates OpenTelemetry-compatible spans, allowing storage
 *         calls to appear in distributed traces.</li>
 *     <li><strong>Logging:</strong> Correlation IDs are automatically attached to the MDC
 *         during execution.</li>
 * </ul>
 *
 * <p>Data is recorded with a low-cardinality tag {@code storage.type} derived from
 * the delegate's description.
 *
 * @param <T> The type used for file identifiers.
 * @see ObservationRegistry
 * @see FileStorageServiceWrapper
 */
public class ObservationFileStorageServiceWrapper<T> implements FileStorageServiceWrapper<T> {

    private final FileStorageService<T> delegate;
    private final String storageType;

    private ObservationRegistry observationRegistry;

    public ObservationFileStorageServiceWrapper(final FileStorageService<T> delegate) {
        this.delegate = delegate;
        this.storageType = delegate.getStorageDescription().toLowerCase(Locale.ROOT).replace(" ", "-");
    }

    @Override
    public FileStorageService<T> getService() {
        return delegate;
    }

    @Override
    public boolean exists(final T id) throws FileStorageException {
        return buildObservation("exists").observeChecked(() -> delegate.exists(id));
    }

    @Override
    public long getSize(final T id) throws FileStorageException {
        return buildObservation("get_size").observeChecked(() -> delegate.getSize(id));
    }

    @Override
    public void create(final T id, final byte[] bytes) throws FileStorageException {
        buildObservation("create").observeChecked(() -> delegate.create(id, bytes));
    }

    @Override
    public void create(final T id, final InputStream inputStream, final long contentSize) throws FileStorageException {
        buildObservation("create_stream").observeChecked(() -> delegate.create(id, inputStream, contentSize));
    }

    @Override
    public void delete(final T id) throws FileStorageException {
        buildObservation("delete").observeChecked(() -> delegate.delete(id));
    }

    @Override
    public byte[] getBytes(final T id) throws FileStorageException {
        return buildObservation("get_bytes").observeChecked(() -> delegate.getBytes(id));
    }

    @Override
    public byte[] getBytes(final T id, final long startPosition, final long endPosition) throws FileStorageException {
        return buildObservation("get_bytes_range").observeChecked(() -> delegate.getBytes(id, startPosition, endPosition));
    }

    @Override
    public InputStream getInputStream(final T id) throws FileStorageException {
        return buildObservation("get_input_stream").observeChecked(() -> delegate.getInputStream(id));
    }

    @Override
    public void deleteAll() throws FileStorageException {
        buildObservation("delete_all").observeChecked(delegate::deleteAll);
    }

    @Override
    public FileStorageIdService<T> getFileStorageIdService() {
        return delegate.getFileStorageIdService();
    }

    /**
     * Helper to build a standardized Observation for the given operation.
     */
    private Observation buildObservation(String operation) {
        return Observation.createNotStarted("file.storage", observationRegistry)
                .lowCardinalityKeyValue("storage.type", storageType)
                .lowCardinalityKeyValue("operation", operation);
    }

    @Autowired
    public void setObservationRegistry(final ObservationRegistry observationRegistry) {
        this.observationRegistry = observationRegistry;
    }
}