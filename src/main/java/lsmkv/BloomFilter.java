package lsmkv_old2.lsmkv;

import java.nio.ByteBuffer;
import java.util.Random;

public final class BloomFilter {
    private final byte[] bits;
    private final int k;

    public BloomFilter(int bitsPerKey, int numKeys) {
        int nbits = Math.max(64, nextPow2(numKeys * bitsPerKey));
        this.bits = new byte[nbits / 8];
        this.k = Math.max(1, (int)Math.round(bitsPerKey * Math.log(2)));
    }

    private static int nextPow2(int x) {
        int v = 1;
        while (v < x) v <<= 1;
        return v;
    }

    private int mix(byte[] key, int seed) {
        int h = seed;
        for (byte b : key) {
            h ^= (b & 0xff);
            h *= 0x5bd1e995;
            h ^= h >>> 13;
        }
        return h;
    }

    public void add(byte[] key) {
        for (int i = 0; i < k; i++) {
            int h = mix(key, i * 0x9e3779b1);
            int bit = (h & 0x7fffffff) % (bits.length * 8);
            bits[bit >>> 3] |= (1 << (bit & 7));
        }
    }

    public boolean mightContain(byte[] key) {
        for (int i = 0; i < k; i++) {
            int h = mix(key, i * 0x9e3779b1);
            int bit = (h & 0x7fffffff) % (bits.length * 8);
            if ((bits[bit >>> 3] & (1 << (bit & 7))) == 0) return false;
        }
        return true;
    }

    public byte[] toBytes() { return bits; }
    public static BloomFilter fromBytes(byte[] bits) {
        BloomFilter f = new BloomFilter(10, bits.length * 8);
        System.arraycopy(bits, 0, f.bits, 0, Math.min(bits.length, f.bits.length));
        return f;
    }
}
