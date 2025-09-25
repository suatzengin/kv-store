package lsmkv;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

final class MemTable {
    private final ConcurrentSkipListMap<byte[], Entry> map = new ConcurrentSkipListMap<>(ByteArrays.LEXICOGRAPHIC);
    private volatile long approxBytes = 0L;

    void put(Entry e) {
        Entry prev = map.put(e.key, e);
        long delta = (e.key.length + (e.value == null ? 0 : e.value.length) + 32);
        if (prev != null) delta -= (prev.key.length + (prev.value == null ? 0 : prev.value.length) + 32);
        approxBytes += delta;
    }

    Optional<Entry> get(byte[] key) {
        Entry e = map.get(key);
        return Optional.ofNullable(e);
    }

    void delete(Entry e) { put(e); }

    boolean isOverSize(int maxBytes) { return approxBytes > maxBytes; }

    NavigableMap<byte[], Entry> subMap(byte[] startIncl, byte[] endExcl) {
        return map.subMap(startIncl, true, endExcl, false);
    }

    SortedMap<byte[], Entry> snapshot() {
        SortedMap<byte[], Entry> snap = new TreeMap<>(ByteArrays.LEXICOGRAPHIC);
        snap.putAll(map);
        return snap;
    }

    void clear() { map.clear(); approxBytes = 0; }

    int size() { return map.size(); }
}