package lsmkv_old2.lsmkv;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class MemTable {
    public final ConcurrentSkipListMap<byte[], Entry> map = new ConcurrentSkipListMap<>(ByteArrays.LEX);
    private volatile int approxBytes = 0;
    public void put(Entry e) {
        Entry prev = map.put(e.key, e);
        int delta = (e.key.length + (e.value == null ? 0 : e.value.length) + 32) -
            (prev == null ? 0 : (prev.key.length + (prev.value == null ? 0 : prev.value.length) + 32));
        approxBytes += delta;
    }
    public int approxBytes() { return approxBytes; }
    public NavigableMap<byte[], Entry> subMap(byte[] start, byte[] end) { return map.subMap(start, true, end, false); }
    public Entry get(byte[] key) { return map.get(key); }
}
