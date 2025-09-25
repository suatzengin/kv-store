package lsmkv;

import java.util.Arrays;
import java.util.Comparator;

final class ByteArrays {
    static final Comparator<byte[]> LEXICOGRAPHIC = (a, b) -> {
        int min = Math.min(a.length, b.length);
        for (int i = 0; i < min; i++) {
            int ai = a[i] & 0xFF;
            int bi = b[i] & 0xFF;
            if (ai != bi) return ai - bi;
        }
        return a.length - b.length;
    };

    static boolean equals(byte[] a, byte[] b) { return Arrays.equals(a, b); }
}