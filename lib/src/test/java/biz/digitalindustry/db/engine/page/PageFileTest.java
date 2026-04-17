package biz.digitalindustry.db.engine.page;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertArrayEquals;

public class PageFileTest {
    @Test
    public void testCorruptLatestSlotCheckpointFallsBackToPreviousValidSlotCheckpoint() throws Exception {
        Path dbPath = Files.createTempFile("page-file-checkpoint", ".dbs");
        Files.deleteIfExists(dbPath);

        PageFile pageFile = new PageFile(dbPath, 8192);
        try {
            pageFile.open();
            byte[] firstSlotPayload = "slot-checkpoint-1".getBytes(StandardCharsets.UTF_8);
            byte[] secondSlotPayload = "slot-checkpoint-2".getBytes(StandardCharsets.UTF_8);

            pageFile.writePayload(firstSlotPayload);
            pageFile.writePayload(secondSlotPayload);
            pageFile.corruptLatestPayloadByteForTest();

            assertArrayEquals(firstSlotPayload, pageFile.readPayload());
        } finally {
            pageFile.close();
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testInterruptedSlotCheckpointWithoutDescriptorFallsBackAndNextWriteAdvances() throws Exception {
        Path dbPath = Files.createTempFile("page-file-incomplete-slot", ".dbs");
        Files.deleteIfExists(dbPath);

        PageFile pageFile = new PageFile(dbPath, 8192);
        try {
            pageFile.open();
            byte[] firstPayload = "slot-checkpoint-1".getBytes(StandardCharsets.UTF_8);
            byte[] interruptedPayload = "slot-checkpoint-interrupted".getBytes(StandardCharsets.UTF_8);
            byte[] finalPayload = "slot-checkpoint-2".getBytes(StandardCharsets.UTF_8);

            pageFile.writePayload(firstPayload);
            pageFile.writeIncompleteSlotCheckpointForTest(interruptedPayload);
            pageFile.close();

            pageFile = new PageFile(dbPath, 8192);
            pageFile.open();
            assertArrayEquals(firstPayload, pageFile.readPayload());

            pageFile.writePayload(finalPayload);
            assertArrayEquals(finalPayload, pageFile.readPayload());
        } finally {
            pageFile.close();
            Files.deleteIfExists(dbPath);
        }
    }

    @Test
    public void testCorruptPublishedSlotCheckpointFallsBackAndNextWriteAdvances() throws Exception {
        Path dbPath = Files.createTempFile("page-file-corrupt-slot", ".dbs");
        Files.deleteIfExists(dbPath);

        PageFile pageFile = new PageFile(dbPath, 8192);
        try {
            pageFile.open();
            byte[] firstPayload = "slot-checkpoint-1".getBytes(StandardCharsets.UTF_8);
            byte[] corruptPayload = "slot-checkpoint-corrupt".getBytes(StandardCharsets.UTF_8);
            byte[] finalPayload = "slot-checkpoint-2".getBytes(StandardCharsets.UTF_8);

            pageFile.writePayload(firstPayload);
            pageFile.writeCorruptSlotCheckpointForTest(corruptPayload);
            pageFile.close();

            pageFile = new PageFile(dbPath, 8192);
            pageFile.open();
            assertArrayEquals(firstPayload, pageFile.readPayload());

            pageFile.writePayload(finalPayload);
            assertArrayEquals(finalPayload, pageFile.readPayload());
        } finally {
            pageFile.close();
            Files.deleteIfExists(dbPath);
        }
    }
}
