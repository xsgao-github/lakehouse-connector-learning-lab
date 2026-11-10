package xsgao.lakehouseconnector;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StorageSmokeTest {
    private StorageSmokeTestApp app;

    @BeforeEach
    public void setUp() {
        app = new StorageSmokeTestApp();
    }

    @Test
    public void testWriteAndReadRoundTrip() {
        // 1. Explicitly test the write operation
        app.writeTestFile();

        // 2. Explicitly test the read operation and validate content
        String content = app.readTestFile();
        assertThat(content).isNotNull();
        assertThat(content).contains("order_id,amount,order_date");
        assertThat(content).contains("1001,150.50");
    }
}