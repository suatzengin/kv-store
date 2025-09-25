package lsmkv_old2.lsmkv;

public final class Entry {
    public static final byte FLAG_PUT = 1;
    public static final byte FLAG_DEL = 2;

    public final long seq;
    public final byte flag;
    public final byte[] key;
    public final byte[] value; // null for delete

    public Entry(long seq, byte flag, byte[] key, byte[] value) {
        this.seq = seq;
        this.flag = flag;
        this.key = key;
        this.value = value;
    }
}
