package lsmkv_old2.lsmkv;

import java.util.ArrayList;
import java.util.List;

public final class SparseIndex {
    public static final class Entry {
        public final byte[] key;
        public final long offset;
        public Entry(byte[] key, long offset) { this.key = key; this.offset = offset; }
    }
    private final List<Entry> list = new ArrayList<>();
    public void add(byte[] key, long off) { list.add(new Entry(key, off)); }
    public List<Entry> entries() { return list; }
}
