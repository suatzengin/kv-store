package lsmkv_old2.lsmkv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

public final class SstReader implements AutoCloseable {
    private final Path path;

    public SstReader(Path path) {
        this.path = path;
    }

    public Optional<Entry> get(byte[] key) throws IOException {
        // naive scan with per-block crc validation; in practice use index+bloom for narrowing (omitted for brevity)
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            long size = ch.size();
            long pos = 0;
            ByteBuffer hdr = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
            while (pos + 8 <= size) {
                hdr.clear();
                int r = ch.read(hdr, pos);
                if (r < 8) break;
                hdr.flip();
                int crc = hdr.getInt();
                int blen = hdr.getInt();
                if (pos + 8 + blen > size) break;
                ByteBuffer body = ByteBuffer.allocate(blen).order(ByteOrder.LITTLE_ENDIAN);
                ch.read(body, pos + 8);
                body.flip();
                // verify crc
                byte[] arr = new byte[body.remaining()];
                body.get(arr);
                if (Codec.crc32c(arr) != crc) break;
                // parse entries in block
                ByteBuffer bb = ByteBuffer.wrap(arr).order(ByteOrder.LITTLE_ENDIAN);
                while (bb.hasRemaining()) {
                    byte flag = bb.get();
                    int klen = bb.getInt();
                    int vlen = bb.getInt();
                    byte[] k = new byte[klen];
                    bb.get(k);
                    byte[] v = new byte[vlen];
                    if (vlen > 0) bb.get(v);
                    int cmp = ByteArrays.compare(k, key);
                    if (cmp == 0) {
                        if (flag == Entry.FLAG_DEL) return Optional.empty();
                        return Optional.of(new Entry(0, flag, k, vlen == 0 ? null : v));
                    }
                }
                pos += 8 + blen;
            }
        }
        return Optional.empty();
    }

    @Override public void close() throws IOException {}
}
