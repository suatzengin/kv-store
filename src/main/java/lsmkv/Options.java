package lsmkv;

import java.nio.file.Path;

public final class Options {
    public final Path rootDir;
    public final Path walDir;
    public final Path sstDir;
    public final int memTableMaxBytes;
    public final int maxImmutables;
    public final int walBatchBytes;
    public final int walSyncMillis;
    public final int sstBlockSizeBytes;
    public final int bloomBitsPerKey;
    public final int flushParallelism;
    public final int compactionTriggerFiles;

    public Options(Path rootDir, int memTableMaxBytes, int maxImmutables, int walBatchBytes, int walSyncMillis,
                   int sstBlockSizeBytes, int bloomBitsPerKey,
                   int flushParallelism, int compactionTriggerFiles) {
        this.rootDir = rootDir;
        this.walDir = rootDir.resolve("wal");
        this.sstDir = rootDir.resolve("sst");
        this.memTableMaxBytes = memTableMaxBytes;
        this.maxImmutables = maxImmutables;
        this.walBatchBytes = walBatchBytes;
        this.walSyncMillis = walSyncMillis;
        this.sstBlockSizeBytes = sstBlockSizeBytes;
        this.bloomBitsPerKey = bloomBitsPerKey;
        this.flushParallelism = flushParallelism;
        this.compactionTriggerFiles = compactionTriggerFiles;
    }

    public static Options defaults(Path root) {
        return new Options(
                root,
                64 * 1024 * 1024, // memtable 64MB
                6,                // at most 6 immutable memtables
                1 * 1024 * 1024,  // wal batch bytes
                10,               // wal fsync every 10ms
                16 * 1024,        // sst block
                10,               // bloom bits per key
                2,                // parallel flushes
                10                // trigger compaction when >10 files
        );
    }
}
