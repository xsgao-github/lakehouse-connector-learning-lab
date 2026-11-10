package xsgao.lakehouseconnector.writer;

/**
 * Exception thrown when an atomic write operation fails due to a concurrency conflict.
 */
public class AtomicWriteException extends RuntimeException {
    private final int currentVersion;

    public AtomicWriteException(String message, int currentVersion) {
        super(message);
        this.currentVersion = currentVersion;
    }

    public int getCurrentVersion() {
        return currentVersion;
    }
}
