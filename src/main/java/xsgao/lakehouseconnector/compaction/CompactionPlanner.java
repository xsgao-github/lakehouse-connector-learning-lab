package xsgao.lakehouseconnector.compaction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import xsgao.lakehouseconnector.catalog.DataFile;

/**
 * Pure compaction policy: decides which small files to rewrite and how to bin them into
 * target-sized output files. No S3 or Parquet I/O happens here, so the policy is unit-testable.
 */
public final class CompactionPlanner {

    /**
     * Plans one {@link Group} per partition that has enough small files. Each bin inside a
     * group maps to exactly one output file.
     *
     * @param liveFiles files referenced by the active snapshot
     * @param options   thresholds and target file size
     * @return qualifying groups sorted by partition; empty when nothing should be rewritten
     */
    public List<Group> plan(List<DataFile> liveFiles, CompactionOptions options) {
        if (liveFiles == null || liveFiles.isEmpty()) {
            return List.of();
        }

        // Partition pruning in this lab is path-based, so the parent directory is the partition.
        Map<String, List<DataFile>> byPartition = new TreeMap<>();
        for (DataFile file : liveFiles) {
            byPartition.computeIfAbsent(partitionOf(file), key -> new ArrayList<>()).add(file);
        }

        List<Group> groups = new ArrayList<>();
        for (Map.Entry<String, List<DataFile>> entry : byPartition.entrySet()) {
            List<DataFile> smallFiles = new ArrayList<>();
            for (DataFile file : entry.getValue()) {
                if (file.getFileSizeInBytes() < options.getSmallFileThresholdBytes()) {
                    smallFiles.add(file);
                }
            }

            if (smallFiles.size() < options.getMinInputFilesPerGroup()) {
                continue;
            }
            groups.add(new Group(entry.getKey(), binPack(smallFiles, options.getTargetFileSizeBytes())));
        }
        return List.copyOf(groups);
    }

    private static String partitionOf(DataFile file) {
        String path = file.getFilePath();
        if (path == null) {
            return "";
        }
        int slash = path.lastIndexOf('/');
        return slash < 0 ? "" : path.substring(0, slash);
    }

    /**
     * First-fit-decreasing: largest files first, then the first bin with enough remaining
     * capacity. Deterministic and simple; an input larger than the target gets its own bin.
     */
    private static List<List<DataFile>> binPack(List<DataFile> smallFiles, long targetFileSizeBytes) {
        List<DataFile> sorted = new ArrayList<>(smallFiles);
        sorted.sort(Comparator.comparingLong(DataFile::getFileSizeInBytes).reversed()
                .thenComparing(DataFile::getFilePath));

        List<List<DataFile>> bins = new ArrayList<>();
        List<Long> used = new ArrayList<>();

        for (DataFile file : sorted) {
            long size = file.getFileSizeInBytes();
            boolean placed = false;
            for (int i = 0; i < bins.size(); i++) {
                if (used.get(i) + size <= targetFileSizeBytes) {
                    bins.get(i).add(file);
                    used.set(i, used.get(i) + size);
                    placed = true;
                    break;
                }
            }
            if (!placed) {
                List<DataFile> bin = new ArrayList<>();
                bin.add(file);
                bins.add(bin);
                used.add(size);
            }
        }
        return bins;
    }

    /**
     * All small files for one partition, already split into output-sized bins.
     */
    public record Group(String partition, List<List<DataFile>> bins) {

        public Group {
            partition = partition == null ? "" : partition;
            List<List<DataFile>> defensiveCopy = new ArrayList<>();
            for (List<DataFile> bin : bins) {
                defensiveCopy.add(List.copyOf(bin));
            }
            bins = List.copyOf(defensiveCopy);
        }

        public List<DataFile> inputs() {
            List<DataFile> flattened = new ArrayList<>();
            for (List<DataFile> bin : bins) {
                flattened.addAll(bin);
            }
            return List.copyOf(flattened);
        }

        public int inputFileCount() {
            int count = 0;
            for (List<DataFile> bin : bins) {
                count += bin.size();
            }
            return count;
        }
    }
}
