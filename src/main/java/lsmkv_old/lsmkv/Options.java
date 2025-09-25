package lsmkv;

import java.nio.file.Path;

public record Options(Path dataDir, Path walDir, int memTableMaxBytes, int walBatchBytes, int walSyncMillis) {

    public static Options defaults(Path root) {
        return new Options(
                root.resolve("data"),
                root.resolve("wal"),
                64 * 1024 * 1024, // 64MB memtable
                1 * 1024 * 1024, // 1MB WAL group commit batch
                10 // 10ms group-fsync window
        );
    }
}
