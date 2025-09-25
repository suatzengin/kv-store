package lsmkv;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

public final class LsmKvStore implements KvStore {
    private final Options opt;
    private final Seq seq = new Seq();
    private volatile MemTable active = new MemTable();
    private final List<SortedMap<byte[], Entry>> immutables = new CopyOnWriteArrayList<>();
    private final List<SstReader> sstReaders = new CopyOnWriteArrayList<>(); // newest first
    private final Wal wal;
    private final ExecutorService flushExec = Executors.newSingleThreadExecutor(r -> { var t = new Thread(r, "flush"); t.setDaemon(true); return t;});

    public LsmKvStore(Options options) throws Exception {
        this.opt = options;
        Files.createDirectories(opt.dataDir);
        Files.createDirectories(opt.walDir);
        Files.createDirectories(opt.sstDir);
        this.wal = new Wal(opt.walDir, opt.walBatchBytes, opt.walSyncMillis);
        recover();
        loadExistingSstables();
    }

    private void loadExistingSstables() throws IOException {
        if (!Files.exists(opt.sstDir)) return;
        try (var ds = Files.newDirectoryStream(opt.sstDir, "*.sst")) {
            List<Path> files = new ArrayList<>();
            for (Path p : ds) files.add(p);
            files.sort(Comparator.comparing(Path::getFileName));
// newest first
            Collections.reverse(files);
            for (Path p : files) {
                sstReaders.add(SstReader.open(p));
            }
        }
    }

    private void recover() throws IOException {
        List<Entry> entries = wal.replayAll();
        long maxSeq = 0L;
        for (Entry e : entries) {
            if (e.seq > maxSeq) maxSeq = e.seq;
            if (e.flag == Entry.FLAG_PUT) active.put(e); else active.delete(e);
        }
        if (maxSeq > 0) seq.jumpTo(maxSeq + 1);
    }

    @Override public void put(byte[] key, byte[] value) throws Exception {
        Objects.requireNonNull(key); Objects.requireNonNull(value);
        long s = seq.next();
        Entry e = new Entry(s, Entry.FLAG_PUT, key, value);
        wal.append(e);
        active.put(e);
        maybeFlushAsync();
    }

    @Override public Optional<byte[]> read(byte[] key) throws Exception {
        Objects.requireNonNull(key);
// 1) Active memtable
        Optional<Entry> e = active.get(key);
        if (e.isPresent()) return e.get().flag == Entry.FLAG_DEL ? Optional.empty() : Optional.of(e.get().value);
// 2) Immutables (newest first)
        for (var imm : immutables) {
            Entry ee = imm.get(key);
            if (ee != null) return ee.flag == Entry.FLAG_DEL ? Optional.empty() : Optional.of(ee.value);
        }
// 3) SSTables (newest first)
        for (SstReader r : sstReaders) {
            Optional<Entry> hit = r.get(key);
            if (hit.isPresent()) {
                Entry ee = hit.get();
                return ee.flag == Entry.FLAG_DEL ? Optional.empty() : Optional.ofNullable(ee.value);
            }
        }
        return Optional.empty();
    }

    @Override public List<KeyValue> readKeyRange(byte[] start, byte[] end) throws Exception {
// MVP: merge only the active memtable for range; add SSTable range in a later step
        NavigableMap<byte[], Entry> sub = active.subMap(start, end);
        List<KeyValue> out = new ArrayList<>(sub.size());
        for (Entry ent : sub.values()) if (ent.flag == Entry.FLAG_PUT) out.add(new KeyValue(ent.key, ent.value));
// Also scan immutables newest first and append if not shadowed by active
        for (var imm : immutables) {
            var sub2 = imm.subMap(start, end);
            for (var ent : sub2.values()) {
                if (ent.flag == Entry.FLAG_PUT && active.get(ent.key).isEmpty()) out.add(new KeyValue(ent.key, ent.value));
            }
        }
// NOTE: Full range merging across SSTables will come with iterators in compaction step
        return out;
    }

    @Override public void batchPut(List<byte[]> keys, List<byte[]> values) throws Exception {
        Objects.requireNonNull(keys); Objects.requireNonNull(values);
        if (keys.size() != values.size()) throw new IllegalArgumentException("keys.size != values.size");
        for (int i = 0; i < keys.size(); i++) put(keys.get(i), values.get(i));
    }

    @Override public void delete(byte[] key) throws Exception {
        Objects.requireNonNull(key);
        long s = seq.next();
        Entry e = new Entry(s, Entry.FLAG_DEL, key, null);
        wal.append(e);
        active.delete(e);
        maybeFlushAsync();
    }

    private void maybeFlushAsync() {
        if (active.isOverSize(opt.memTableMaxBytes)) {
// rotate
            SortedMap<byte[], Entry> snap = active.snapshot();
            immutables.add(0, snap); // newest first
            active.clear();
            flushExec.submit(() -> flushImmutable(snap));
        }
    }

    private void flushImmutable(SortedMap<byte[], Entry> snap) {
        try {
            String name = "L0-" + System.nanoTime() + ".sst";
            Path out = opt.sstDir.resolve(name);
            SstWriter.write(out, snap.entrySet().iterator(), snap.size(), opt);
// After success, drop from immutables and add reader at head
            immutables.remove(snap);
            sstReaders.add(0, SstReader.open(out));
// TODO: roll WAL segment & rewrite manifest in a later step
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        flushExec.shutdown();
        try {
            flushExec.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        wal.close();
        for (var r : sstReaders) r.close();
    }
}