package lsmkv_old2.lsmkv;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32C;

public final class Codec {
    private Codec() {}

    public static void putVarInt(ByteBuffer buf, int v) {
        long x = Integer.toUnsignedLong(v);
        while ((x & ~0x7FL) != 0) {
            buf.put((byte)((x & 0x7F) | 0x80));
            x >>>= 7;
        }
        buf.put((byte)x);
    }

    public static int getVarInt(ByteBuffer buf) {
        int shift = 0;
        int result = 0;
        while (true) {
            byte b = buf.get();
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        return result;
    }

    public static int crc32c(byte[] data) {
        CRC32C c = new CRC32C();
        c.update(data, 0, data.length);
        return (int)c.getValue();
    }

    public static int crc32c(byte[] a, int off, int len) {
        CRC32C c = new CRC32C();
        c.update(a, off, len);
        return (int)c.getValue();
    }
}
