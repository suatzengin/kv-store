package lsmkv;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class MemTable {

    public final ConcurrentSkipListMap<byte[], Entry> map = new ConcurrentSkipListMap<>(ByteArrays.LEX);
    private final AtomicInteger approxBytes = new AtomicInteger(0);

    public void put(Entry entry) {
        Entry prev = map.put(entry.key(), entry);
        int delta = (entry.key().length + (entry.value() == null ? 0 : entry.value().length) + 32) -
                (prev == null ? 0 : (prev.key().length + (prev.value() == null ? 0 : prev.value().length) + 32));
        approxBytes.addAndGet(delta);
    }

    public int approxBytes() {
        return approxBytes.get();
    }

    public NavigableMap<byte[], Entry> subMap(byte[] start, byte[] end) {
        return map.subMap(start, true, end, false);
    }

    public Entry get(byte[] key) {
        return map.get(key);
    }
}
