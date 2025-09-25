package lsmkv;

public final class BloomFilter {
    private final byte[] bits;
    private final int k;

    public BloomFilter(int bitsPerKey, int numKeys) {
        int nbits = Math.max(64, nextPow2(numKeys * bitsPerKey));
        this.bits = new byte[nbits / 8];
        this.k = Math.max(1, (int) Math.round(bitsPerKey * Math.log(2)));
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

    public byte[] toBytes() {
        return bits;
    }
}
