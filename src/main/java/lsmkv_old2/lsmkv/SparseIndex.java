package lsmkv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

final class SparseIndex {
    private final List<byte[]> firstKeys = new ArrayList<>();
    private final List<Long> offsets = new ArrayList<>();

    void add(byte[] firstKey, long offset) { firstKeys.add(firstKey); offsets.add(offset); }

    long seekBlockOffset(byte[] key) {
// binary search for the last firstKey <= key
        int lo = 0, hi = firstKeys.size() - 1, ans = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int c = ByteArrays.LEXICOGRAPHIC.compare(firstKeys.get(mid), key);
            if (c <= 0) { ans = mid; lo = mid + 1; } else { hi = mid - 1; }
        }
        return ans >= 0 ? offsets.get(ans) : -1L;
    }

    void writeTo(FileChannel ch) throws IOException {
// format: count(varint) [ klen(varint) key bytes offset(8) ]*
        int cap = 5 + firstKeys.stream().mapToInt(k -> 5 + k.length + 8).sum();
        ByteBuffer buf = ByteBuffer.allocate(cap).order(ByteOrder.LITTLE_ENDIAN);
        Codec.putVarInt(buf, firstKeys.size());
        for (int i = 0; i < firstKeys.size(); i++) {
            byte[] k = firstKeys.get(i);
            Codec.putVarInt(buf, k.length);
            buf.put(k);
            buf.putLong(offsets.get(i));
        }
        buf.flip();
        ch.write(buf);
    }

    static SparseIndex readFrom(FileChannel ch, long off, long len) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate((int)len).order(ByteOrder.LITTLE_ENDIAN);
        ch.read(buf, off);
        buf.flip();
        int n = Codec.getVarInt(buf);
        SparseIndex idx = new SparseIndex();
        for (int i = 0; i < n; i++) {
            int klen = Codec.getVarInt(buf);
            byte[] k = new byte[klen]; buf.get(k);
            long o = buf.getLong();
            idx.add(k, o);
        }
        return idx;
    }
}