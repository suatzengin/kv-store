package lsmkv;

public record Entry(long seq, byte flag, byte[] key, byte[] value) {
    public static final byte FLAG_PUT = 1;
    public static final byte FLAG_DEL = 2;
}
