package lsmkv;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public final class LsmKvStore implements KvStore {
    private final Options options;
    private final Seq seq = new Seq();
    private final Wal wal;
    private final Manifest manifest;
    private final ExecutorService flushExecutor;
    private final ExecutorService compactExecutor;
    private volatile MemTable memTable = new MemTable();
    private final Deque<MemTable> immutables = new ConcurrentLinkedDeque<>();
    private final List<Path> sstFiles = new CopyOnWriteArrayList<>();
    private final List<SstReader> sstReaders = new CopyOnWriteArrayList<>();

    public LsmKvStore(Options opt) throws IOException {
        options = opt;
        Files.createDirectories(options.rootDir);
        Files.createDirectories(options.walDir);
        Files.createDirectories(options.sstDir);
        wal = new Wal(options.walDir, options.walBatchBytes, options.walSyncMillis);
        manifest = new Manifest(options.rootDir);
        flushExecutor = Executors.newFixedThreadPool(options.flushParallelism, runnable -> {
            Thread thread = new Thread(runnable, "flush");
            thread.setDaemon(true);
            return thread;
        });
        compactExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "compact");
            thread.setDaemon(true);
            return thread;
        });

        // Load MANIFEST
        Manifest.State state = manifest.load();
        seq.setAtLeast(state.lastSeq());
        for (String file : state.sstFiles()) {
            Path path = options.sstDir.resolve(file);
            if (Files.exists(path)) {
                sstFiles.add(path);
                sstReaders.add(new SstReader(path));
            }
        }

        // Replay WAL (sorted)
        for (Entry entry : wal.replayAllSorted()) {
            if (entry.seq() >= seq.get()) seq.setAtLeast(entry.seq());
            memTable.put(entry);
            rollIfNeeded(false);
        }
    }

    private void rollIfNeeded(boolean force) {
        if (!force && memTable.approxBytes() < options.memTableMaxBytes) return;
        MemTable toFlush = memTable;
        memTable = new MemTable();
        immutables.addFirst(toFlush);
        // TODO backpressure: block if too many immutables, in reality use blocking queue
        while (immutables.size() > options.maxImmutables) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException ignored) {
            }
        }
        flushExecutor.submit(() -> flushMemtable(toFlush));
    }

    private void flushMemtable(MemTable mt) {
        try {
            if (mt.map.isEmpty()) return;
            String name = "sst-" + System.nanoTime() + ".sst";
            Path path;
            try (SstWriter writer = new SstWriter(options.sstDir, name, options.sstBlockSizeBytes, options.bloomBitsPerKey)) {
                writer.writeAll(mt.map);
                path = writer.install();
            }
            synchronized (this) {
                sstFiles.add(path);
                sstReaders.add(new SstReader(path));
                immutables.remove(mt);
                // commit manifest with new list
                List<String> names = new ArrayList<>();
                for (Path sstPath : sstFiles) names.add(sstPath.getFileName().toString());
                manifest.commit(names, seq.get());
            }
            // TODO truncate WAL (simple: delete older segments, in reality it should be deleting up to the sequence of flushed sst)
            wal.truncateUpTo(seq.get());
            maybeCompactAsync();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void maybeCompactAsync() {
        if (sstFiles.size() > options.compactionTriggerFiles) {
            compactExecutor.submit(this::runCompaction);
        }
    }

    private void runCompaction() {
        // TODO: implement real compaction (merging multiple sst files into one, removing deleted/overridden entries)
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        Entry entry = new Entry(seq.next(), Entry.FLAG_PUT, key, value);
        wal.append(entry);
        memTable.put(entry);
        rollIfNeeded(false);
    }

    @Override
    public void delete(byte[] key) throws IOException {
        Objects.requireNonNull(key);
        Entry entry = new Entry(seq.next(), Entry.FLAG_DEL, key, null);
        wal.append(entry);
        memTable.put(entry);
        rollIfNeeded(false);
    }

    @Override
    public Optional<byte[]> read(byte[] key) throws IOException {
        Entry entry = memTable.get(key);
        if (entry != null) return entry.flag() == Entry.FLAG_PUT ? Optional.of(entry.value()) : Optional.empty();
        for (MemTable mt : immutables) {
            Entry entry2 = mt.get(key);
            if (entry2 != null) return entry2.flag() == Entry.FLAG_PUT ? Optional.of(entry2.value()) : Optional.empty();
        }
        // newest to oldest
        for (int i = sstReaders.size() - 1; i >= 0; i--) {
            Optional<Entry> value = sstReaders.get(i).get(key);
            if (value.isPresent()) {
                Entry ent = value.get();
                return ent.flag() == Entry.FLAG_PUT ? Optional.of(ent.value()) : Optional.empty();
            }
        }
        return Optional.empty();
    }

    @Override
    public void batchPut(List<byte[]> keys, List<byte[]> values) throws IOException {
        if (keys.size() != values.size()) throw new IllegalArgumentException("mismatched sizes");
        for (int i = 0; i < keys.size(); i++) put(keys.get(i), values.get(i));
    }

    @Override
    public List<KeyValue> readKeyRange(byte[] start, byte[] end) {
        // TODO currently active memtable only; no immutables & sst range scan implemented
        var sub = memTable.subMap(start, end);
        List<KeyValue> out = new ArrayList<>();
        for (Entry ent : sub.values()) if (ent.flag() == Entry.FLAG_PUT) out.add(new KeyValue(ent.key(), ent.value()));
        return out;
    }

    @Override
    public void close() throws IOException {
        rollIfNeeded(true);
        flushExecutor.shutdown();
        compactExecutor.shutdown();
        try {
            flushExecutor.awaitTermination(2, TimeUnit.SECONDS);
            compactExecutor.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
        wal.close();
    }
}
