package lsmkv;

import java.util.zip.CRC32C;

public final class Codec {
    private Codec() {
    }

    public static int crc32c(byte[] data) {
        return crc32c(data, 0, data.length);
    }

    public static int crc32c(byte[] data, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(data, offset, length);
        return (int) crc.getValue();
    }
}
