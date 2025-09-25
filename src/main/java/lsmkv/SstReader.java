package lsmkv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

public record SstReader(Path path) {

    public Optional<Entry> get(byte[] key) throws IOException {
        // TODO simple scan with per-block crc validation; in practice use index+bloom for narrowing (omitted for brevity)
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long size = channel.size();
            long pos = 0;
            ByteBuffer header = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
            while (pos + 8 <= size) {
                // 1) Read block header
                header.clear();
                int numBytesRead = channel.read(header, pos);
                if (numBytesRead < 8) break;        // short read -> stop
                header.flip();
                int crc = header.getInt();     // expected CRC32C
                int bodyLength = header.getInt();    // body length

                if (pos + 8 + bodyLength > size) break;       // truncated tail -> stop

                // 2) Read block body
                ByteBuffer body = ByteBuffer.allocate(bodyLength).order(ByteOrder.LITTLE_ENDIAN);
                channel.read(body, pos + 8);
                body.flip();

                // 3) Verify block CRC
                byte[] arr = new byte[body.remaining()];
                body.get(arr);
                if (Codec.crc32c(arr) != crc) break;        // corruption -> stop file scan

                // 4) Parse entries inside the block
                ByteBuffer buffer = ByteBuffer.wrap(arr).order(ByteOrder.LITTLE_ENDIAN);
                while (buffer.hasRemaining()) {
                    byte flag = buffer.get();
                    int klen = buffer.getInt();
                    int vlen = buffer.getInt();
                    byte[] k = new byte[klen];
                    buffer.get(k);
                    byte[] v = new byte[vlen];
                    if (vlen > 0) buffer.get(v);
                    if (ByteArrays.compare(k, key) == 0) {
                        if (flag == Entry.FLAG_DEL) return Optional.empty();        // tombstone
                        return Optional.of(new Entry(0, flag, k, vlen == 0 ? null : v));
                    }
                }

                // 5) Advance to next block
                pos += 8 + bodyLength;
            }
        }
        return Optional.empty();
    }
}
