package lsmkv;

/**
 * @param value null if DEL
 */
record Entry(long seq, byte flag, byte[] key, byte[] value) {
    static final byte FLAG_PUT = 0;
    static final byte FLAG_DEL = 1;
}