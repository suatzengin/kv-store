package lsmkv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

final class BloomFilter {
    private final int bits;
    private final int k; // hash functions
    private final byte[] bitset;

    BloomFilter(int estimatedKeys, int bitsPerKey) {
        int b = Math.max(1, estimatedKeys) * Math.max(4, bitsPerKey);
        this.bits = (b + 7) & ~7; // multiple of 8
        this.bitset = new byte[bits / 8];
        this.k = Math.max(1, (int)Math.round(bitsPerKey * Math.log(2))); // ~ ln2 * bpk
    }

    void put(byte[] key) {
        long h1 = hash64(key, 0x9E3779B97F4A7C15L);
        long h2 = hash64(key, 0xC2B2AE3D27D4EB4FL);
        for (int i = 0; i < k; i++) {
            int bit = (int)((h1 + i * h2) & 0x7FFFFFFF) % bits;
            bitset[bit >>> 3] |= (byte)(1 << (bit & 7));
        }
    }

    boolean mayContain(byte[] key) {
        long h1 = hash64(key, 0x9E3779B97F4A7C15L);
        long h2 = hash64(key, 0xC2B2AE3D27D4EB4FL);
        for (int i = 0; i < k; i++) {
            int bit = (int)((h1 + i * h2) & 0x7FFFFFFF) % bits;
            if ((bitset[bit >>> 3] & (1 << (bit & 7))) == 0) return false;
        }
        return true;
    }

    void writeTo(FileChannel ch) throws IOException {
        ByteBuffer hdr = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        hdr.putInt(bits).putInt(k).putInt(bitset.length).flip();
        ch.write(hdr);
        ch.write(ByteBuffer.wrap(bitset));
    }

    static BloomFilter readFrom(FileChannel ch, long off, int len) throws IOException {
        ByteBuffer hdr = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        ch.read(hdr, off); hdr.flip();
        int bits = hdr.getInt();
        int k = hdr.getInt();
        int bytes = hdr.getInt();
        byte[] bs = new byte[bytes];
        ch.read(ByteBuffer.wrap(bs), off + 12);
        BloomFilter bf = new BloomFilter(1, Math.max(1, bits / Math.max(1, (bytes * 8))));
        bf.assign(bits, k, bs);
        return bf;
    }

    private void assign(int bits, int k, byte[] bs) {
// ugly but practical for simple deserialize
        try {
            var fBits = BloomFilter.class.getDeclaredField("bits"); fBits.setAccessible(true); fBits.setInt(this, bits);
            var fK = BloomFilter.class.getDeclaredField("k"); fK.setAccessible(true); fK.setInt(this, k);
            var fBs = BloomFilter.class.getDeclaredField("bitset"); fBs.setAccessible(true); fBs.set(this, bs);
        } catch (Exception ignored) {}
    }

    private static long hash64(byte[] data, long seed) {
        long x = seed; for (byte b : data) { x ^= (b & 0xFF); x *= 0x100000001B3L; x ^= (x >>> 33); }
        x ^= x >>> 29; x *= 0xBF58476D1CE4E5B9L; x ^= x >>> 32; return x;
    }
}
