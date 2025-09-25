package lsmkv_old2.lsmkv;

import java.nio.file.Path;

public final class Options {
    public final Path rootDir;
    public final Path walDir;
    public final Path sstDir;
    public final int memTableMaxBytes;
    public final int maxImmutables;         // backpressure cap
    public final int walBatchBytes;
    public final int walSyncMillis;
    public final int sstBlockSizeBytes;
    public final int sstTargetFileBytes;
    public final int bloomBitsPerKey;
    public final int flushParallelism;
    public final int compactionTriggerFiles;
    public final int compactionFanIn;       // size-tiered N-way

    public Options(Path rootDir, int memTableMaxBytes, int maxImmutables, int walBatchBytes, int walSyncMillis,
                   int sstBlockSizeBytes, int sstTargetFileBytes, int bloomBitsPerKey,
                   int flushParallelism, int compactionTriggerFiles, int compactionFanIn) {
        this.rootDir = rootDir;
        this.walDir = rootDir.resolve("wal");
        this.sstDir = rootDir.resolve("sst");
        this.memTableMaxBytes = memTableMaxBytes;
        this.maxImmutables = maxImmutables;
        this.walBatchBytes = walBatchBytes;
        this.walSyncMillis = walSyncMillis;
        this.sstBlockSizeBytes = sstBlockSizeBytes;
        this.sstTargetFileBytes = sstTargetFileBytes;
        this.bloomBitsPerKey = bloomBitsPerKey;
        this.flushParallelism = flushParallelism;
        this.compactionTriggerFiles = compactionTriggerFiles;
        this.compactionFanIn = compactionFanIn;
    }

    public static Options defaults(Path root) {
        return new Options(
            root,
            64 * 1024 * 1024, // memtable 64MB
            6,                // at most 6 immutable memtables before backpressure
            1 * 1024 * 1024,  // wal batch bytes
            10,               // wal fsync every 10ms
            16 * 1024,        // sst block
            128 * 1024 * 1024,// sst target
            10,               // bloom bits per key
            2,                // parallel flushes
            10,               // trigger compaction when >10 files
            4                 // 4-way size-tiered compaction
        );
    }
}
