package lsmkv;

import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

public final class LsmKvStore implements KvStore {
    private final Seq seq = new Seq();
    private final MemTable mem = new MemTable();
    private final Wal wal;

    public LsmKvStore(Options options) throws IOException {
        Files.createDirectories(options.dataDir());
        Files.createDirectories(options.walDir());
        this.wal = new Wal(options.walDir(), options.walBatchBytes(), options.walSyncMillis());
        recover();
    }

    private void recover() throws IOException {
        List<Entry> entries = wal.replayAll();
        long maxSeq = 0L;
        for (Entry e : entries) {
            if (e.seq() > maxSeq) maxSeq = e.seq();
            if (e.flag() == Entry.FLAG_PUT) mem.put(e);
            else mem.delete(e);
        }
        if (maxSeq > 0) seq.jumpTo(maxSeq + 1);
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        long s = seq.next();
        Entry e = new Entry(s, Entry.FLAG_PUT, key, value);
        wal.append(e);
        mem.put(e);
        // Future step: if mem.isOverSize -> rotate to immutable + flush to SST
    }

    @Override
    public Optional<byte[]> read(byte[] key) {
        Objects.requireNonNull(key);
        // Step 1: only memtable lookup
        return mem.get(key).flatMap(e -> e.flag() == Entry.FLAG_DEL ? Optional.empty() : Optional.of(e.value()));
    }

    @Override
    public List<KeyValue> readKeyRange(byte[] startInclusive, byte[] endExclusive) {
        NavigableMap<byte[], Entry> sub = mem.subMap(startInclusive, endExclusive);
        List<KeyValue> out = new ArrayList<>(sub.size());
        for (Entry e : sub.values()) {
            if (e.flag() == Entry.FLAG_PUT) out.add(new KeyValue(e.key(), e.value()));
        }
        return out;
    }

    @Override
    public void batchPut(List<byte[]> keys, List<byte[]> values) throws IOException {
        Objects.requireNonNull(keys);
        Objects.requireNonNull(values);
        if (keys.size() != values.size()) throw new IllegalArgumentException("keys.size != values.size");
        for (int i = 0; i < keys.size(); i++) {
            put(keys.get(i), values.get(i));
        }
    }

    @Override
    public void delete(byte[] key) throws IOException {
        Objects.requireNonNull(key);
        long s = seq.next();
        Entry e = new Entry(s, Entry.FLAG_DEL, key, null);
        wal.append(e);
        mem.delete(e);
    }

    @Override
    public void close() throws IOException {
        wal.close();
    }
}