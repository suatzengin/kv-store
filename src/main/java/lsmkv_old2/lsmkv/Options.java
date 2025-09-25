package lsmkv;

import java.nio.file.Path;

public final class Options {
    public final Path dataDir;
    public final Path walDir;
    public final Path sstDir;
    public final int memTableMaxBytes;
    public final int walBatchBytes;
    public final int walSyncMillis;
    public final int sstBlockSizeBytes;
    public final int sstTargetFileBytes;
    public final int bloomBitsPerKey;

    public Options(Path dataDir, Path walDir, Path sstDir, int memTableMaxBytes, int walBatchBytes, int walSyncMillis,
                   int sstBlockSizeBytes, int sstTargetFileBytes, int bloomBitsPerKey) {
        this.dataDir = dataDir;
        this.walDir = walDir;
        this.sstDir = sstDir;
        this.memTableMaxBytes = memTableMaxBytes;
        this.walBatchBytes = walBatchBytes;
        this.walSyncMillis = walSyncMillis;
        this.sstBlockSizeBytes = sstBlockSizeBytes;
        this.sstTargetFileBytes = sstTargetFileBytes;
        this.bloomBitsPerKey = bloomBitsPerKey;
    }

    public static Options defaults(Path root) {
        return new Options(
                root.resolve("data"),
                root.resolve("wal"),
                root.resolve("sst"),
                64 * 1024 * 1024, // 64MB memtable
                1 * 1024 * 1024, // 1MB WAL group commit batch
                10, // 10ms group-fsync window
                16 * 1024, // 16KB SSTable block
                128 * 1024 * 1024, // 128MB target SST file
                10 // ~10 bits per key (~1% FP)
        );
    }
}
