package lsmkv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

final class Wal implements AutoCloseable {
    private final Path dir;
    private final int batchBytes;
    private final FileChannel ch;
    private final ByteBuffer writeBuf;
    private long nextSegmentId = 1;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "wal-sync");
        t.setDaemon(true);
        return t;
    });

    Wal(Path dir, int batchBytes, int syncMillis) throws IOException {
        this.dir = dir;
        this.batchBytes = batchBytes;
//        Files.createDirectories(dir);
        this.ch = openNewSegment();
        this.writeBuf = ByteBuffer.allocateDirect(4 + 8 + 1 + 5 + 5 + 0 + 0 + 64 * 1024);
        this.writeBuf.order(ByteOrder.LITTLE_ENDIAN);
        scheduler.scheduleAtFixedRate(this::forceSafe, syncMillis, syncMillis, TimeUnit.MILLISECONDS);
    }

    private FileChannel openNewSegment() throws IOException {
        Path p = dir.resolve("segment-" + (nextSegmentId++) + ".log");
        return FileChannel.open(p, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);
    }

    synchronized void append(Entry e) throws IOException {
        // Encode record into a temporary heap buffer to compute CRC easily
        int klen = e.key().length;
        int vlen = (e.flag() == Entry.FLAG_DEL ? 0 : e.value().length);
        int varKey = Codec.sizeofVarInt(klen);
        int varVal = Codec.sizeofVarInt(vlen);
        int payloadLen = 8 + 1 + varKey + varVal + klen + vlen; // seq+flag+varints+data
        int totalLen = 4 + payloadLen; // crc32 + payload
        ensureCapacity(totalLen);

        int pos = writeBuf.position();
        writeBuf.position(pos + 4); // reserve crc
        writeBuf.putLong(e.seq());
        writeBuf.put(e.flag());
        Codec.putVarInt(writeBuf, klen);
        Codec.putVarInt(writeBuf, vlen);
        writeBuf.put(e.key());
        if (e.flag() == Entry.FLAG_PUT) writeBuf.put(e.value());

        // Compute CRC for payload
        int end = writeBuf.position();
        int payloadSize = end - (pos + 4);
        byte[] tmp = new byte[payloadSize];
        writeBuf.position(pos + 4);
        writeBuf.get(tmp);
        int crc = Codec.crc32(tmp);

        writeBuf.position(pos);
        writeBuf.putInt(crc);
        writeBuf.position(end);

        writeBuf.flip();
        while (writeBuf.hasRemaining()) ch.write(writeBuf);
        writeBuf.clear();

        if (writeBuf.capacity() >= batchBytes) forceSafe(); // capacity is static; rely on timer; keep simple
    }

    private void ensureCapacity(int needed) throws IOException {
        if (needed > writeBuf.remaining()) {
            // Not enough room in the direct buffer: flush it to disk first
            writeBuf.flip();
            while (writeBuf.hasRemaining()) ch.write(writeBuf);
            writeBuf.clear();
        }
    }

    private synchronized void forceSafe() {
        try {
            if (ch != null) ch.force(true);
        } catch (IOException ignore) {
        }
    }

    List<Entry> replayAll() throws IOException {
        List<Entry> out = new ArrayList<>();
        if (!Files.exists(dir)) return out;
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "segment-*.log")) {
            for (Path p : ds) {
                out.addAll(replayFile(p));
            }
        }
        return out;
    }

    private List<Entry> replayFile(Path p) throws IOException {
        List<Entry> out = new ArrayList<>();
        try (FileChannel rch = FileChannel.open(p, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocateDirect((int) Math.min(rch.size(), 4 * 1024 * 1024));
            buf.order(ByteOrder.LITTLE_ENDIAN);
            long pos = 0;
            while (pos < rch.size()) {
                buf.clear();
                int read = rch.read(buf, pos);
                if (read <= 0) break;
                buf.flip();
                while (buf.remaining() >= 4) {
                    buf.mark();
                    int crc = buf.getInt();
                    if (buf.remaining() < 9) {
                        buf.reset();
                        break;
                    }
                    long seq = buf.getLong();
                    byte flag = buf.get();
                    try {
                        int klen = Codec.getVarInt(buf);
                        int vlen = Codec.getVarInt(buf);
                        if (buf.remaining() < klen + vlen) {
                            buf.reset();
                            break;
                        }
                        byte[] k = new byte[klen];
                        buf.get(k);
                        byte[] v = (flag == Entry.FLAG_PUT) ? new byte[vlen] : null;
                        if (flag == Entry.FLAG_PUT) buf.get(v);
                        // Verify CRC
                        int payloadLen = 8 + 1 + Codec.sizeofVarInt(klen) + Codec.sizeofVarInt(vlen) + klen + vlen;
                        ByteBuffer slice = buf.duplicate();
                        slice.position(buf.position() - (klen + (flag == Entry.FLAG_PUT ? vlen : 0)) - Codec.sizeofVarInt(klen) - Codec.sizeofVarInt(vlen) - 1 - 8);
                        byte[] payload = new byte[payloadLen];
                        slice.get(payload);
                        int calc = Codec.crc32(payload);
                        if (calc != crc) {
                            // Truncate on corruption
                            return out;
                        }
                        out.add(new Entry(seq, flag, k, v));
                    } catch (Exception ex) {
                        buf.reset();
                        break; // stop at partial record
                    }
                }
                pos += read;
            }
        }
        return out;
    }

    @Override
    public void close() throws IOException {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupted status
        }
        forceSafe();
        if (ch != null) ch.close();
    }
}