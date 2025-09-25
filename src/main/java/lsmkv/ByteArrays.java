package lsmkv_old2.lsmkv;

import java.util.Arrays;
import java.util.Comparator;

public final class ByteArrays {
    private ByteArrays() {}

    public static int compare(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int ai = a[i] & 0xff;
            int bi = b[i] & 0xff;
            if (ai != bi) return ai - bi;
        }
        return a.length - b.length;
    }

    public static final Comparator<byte[]> LEX = ByteArrays::compare;

    public static byte[] concat(byte[] a, byte[] b) {
        byte[] out = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }
}
