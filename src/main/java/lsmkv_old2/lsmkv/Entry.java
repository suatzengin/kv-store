package lsmkv;

final class Entry {
    static final byte FLAG_PUT = 0;
    static final byte FLAG_DEL = 1;

    final long seq;
    final byte flag;
    final byte[] key;
    final byte[] value; // null if DEL

    Entry(long seq, byte flag, byte[] key, byte[] value) {
        this.seq = seq;
        this.flag = flag;
        this.key = key;
        this.value = value;
    }
}