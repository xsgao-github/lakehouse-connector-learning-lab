package xsgao.lakehouseconnector.catalog;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class TableMetadataTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    public void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Test
    public void testTableMetadataSerializationAndDeserialization() throws Exception {
        // 1. Build Schema
        Field orderId = new Field(1, "order_id", "LONG", true); // Test lowercase normalization
        Field amount = new Field(2, "amount", "double", false);
        Schema schema = new Schema(1, List.of(orderId, amount));

        // 2. Build Snapshot & DataFile
        DataFile dataFile = new DataFile("data/dt=2026-11-03/part-001.parquet", 1500, 45021L);
        Snapshot snapshot = new Snapshot(1001, 1730640000000L, Long.valueOf(1000), List.of(dataFile));

        // 3. Build TableMetadata
        String tableUuid = UUID.randomUUID().toString();
        TableMetadata originalMetadata = new TableMetadata(
                1,
                tableUuid,
                "s3://warehouse/tables/orders",
                schema,
                (long) 1001,
                List.of(snapshot)
        );

        // --- SERIALIZE (Java -> JSON) ---
        String jsonOutput = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(originalMetadata);
        assertNotNull(jsonOutput);
        
        // Verify kebab-case keys are present in the JSON string
        assertTrue(jsonOutput.contains("\"format-version\""));
        assertTrue(jsonOutput.contains("\"table-uuid\""));
        assertTrue(jsonOutput.contains("\"current-snapshot-id\""));
        assertTrue(jsonOutput.contains("\"snapshot-id\""));
        assertTrue(jsonOutput.contains("\"parent-snapshot-id\""));
        assertTrue(jsonOutput.contains("\"added-files\""));

        // --- DESERIALIZE (JSON -> Java) ---
        TableMetadata deserializedMetadata = objectMapper.readValue(jsonOutput, TableMetadata.class);

        // --- VERIFY FIELDS ---
        assertNotNull(deserializedMetadata);
        assertEquals(originalMetadata.getFormatVersion(), deserializedMetadata.getFormatVersion());
        assertEquals(originalMetadata.getTableUuid(), deserializedMetadata.getTableUuid());
        assertEquals(originalMetadata.getLocation(), deserializedMetadata.getLocation());
        assertEquals(originalMetadata.getCurrentSnapshotId(), deserializedMetadata.getCurrentSnapshotId());

        // Verify Schema & Normalized Type
        assertEquals(1, deserializedMetadata.getSchema().getSchemaId());
        assertEquals(2, deserializedMetadata.getSchema().getFields().size());
        assertEquals("long", deserializedMetadata.getSchema().getFields().get(0).getType()); // "LONG" normalized to "long"

        // Verify Snapshot & Null Parent ID
        assertEquals(1, deserializedMetadata.getSnapshots().size());
        Snapshot loadedSnapshot = deserializedMetadata.getSnapshots().get(0);
        assertEquals(1001, loadedSnapshot.getSnapshotId());
        assertEquals(Long.valueOf(1000), loadedSnapshot.getParentSnapshotId()); // Root snapshot parent should be null

        // Verify DataFile
        assertEquals(1, loadedSnapshot.getAddedFiles().size());
        assertEquals("data/dt=2026-11-03/part-001.parquet", loadedSnapshot.getAddedFiles().get(0).getFilePath());
        assertEquals(1500, loadedSnapshot.getAddedFiles().get(0).getRecordCount());
    }
}