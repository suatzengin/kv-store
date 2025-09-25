package lsmkv;

import java.util.ArrayList;
import java.util.List;

public final class SparseIndex {
    public record Entry(byte[] key, long offset) {
    }

    private final List<Entry> list = new ArrayList<>();

    public void add(byte[] key, long off) {
        list.add(new Entry(key, off));
    }

    public List<Entry> entries() {
        return list;
    }
}
