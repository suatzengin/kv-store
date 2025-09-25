package lsmkv;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;

final class Codec {
    static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;

    static void putVarInt(ByteBuffer buf, int value) {
        while ((value & ~0x7F) != 0) {
            buf.put((byte)((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buf.put((byte)value);
    }

    static int getVarInt(ByteBuffer buf) {
        int shift = 0, result = 0; byte b;
        do {
            if (!buf.hasRemaining()) throw new IllegalStateException("Truncated varint");
            b = buf.get();
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }

    static int sizeofVarInt(int value) {
        int bytes = 1; while ((value & ~0x7F) != 0) { bytes++; value >>>= 7; } return bytes;
    }

    static int crc32(byte[] data, int off, int len) {
        CRC32 crc = new CRC32();
        crc.update(data, off, len);
        return (int)crc.getValue();
    }
}