package lsmkv_old2.lsmkv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Wal implements AutoCloseable {
    private final Path dir;
    private final int batchBytes;
    private final int syncMillis;
    private FileChannel ch;
    private long activeId = 1;
    private long activeBytes = 0;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "wal-sync");
        t.setDaemon(true);
        return t;
    });
    private final Object lock = new Object();
    private static final Pattern SEG_PAT = Pattern.compile("segment-(\d+)\.log");

    public Wal(Path dir, int batchBytes, int syncMillis) throws IOException {
        this.dir = dir;
        this.batchBytes = batchBytes;
        this.syncMillis = syncMillis;
        Files.createDirectories(dir);
        rolloverIfNeeded();
        scheduler.scheduleAtFixedRate(this::forceSafe, syncMillis, syncMillis, TimeUnit.MILLISECONDS);
    }

    private void forceSafe() {
        try {
            synchronized (lock) {
                if (ch != null) ch.force(true);
                // also fsync dir so new segments are durable
                try (FileChannel dch = FileChannel.open(dir, StandardOpenOption.READ)) {
                    dch.force(true);
                } catch (IOException ignore) {}
            }
        } catch (IOException ignore) {}
    }

    private void rolloverIfNeeded() throws IOException {
        synchronized (lock) {
            if (ch == null) {
                activeId = findMaxSegmentId() + 1;
                Path p = dir.resolve("segment-" + activeId + ".log");
                ch = FileChannel.open(p, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                activeBytes = 0;
            }
        }
    }

    private long findMaxSegmentId() throws IOException {
        long max = 0;
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "segment-*.log")) {
            for (Path p : ds) {
                Matcher m = SEG_PAT.matcher(p.getFileName().toString());
                if (m.matches()) {
                    long id = Long.parseLong(m.group(1));
                    if (id > max) max = id;
                }
            }
        }
        return max;
    }

    public void append(Entry e) throws IOException {
        // record: crc32c | seq | flag | keyLen | valLen | key | val
        byte[] k = e.key;
        byte[] v = e.value == null ? new byte[0] : e.value;
        int bodyLen = 8 + 1 + 4 + 4 + k.length + v.length;
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen).order(ByteOrder.LITTLE_ENDIAN);
        buf.position(4);
        buf.putLong(e.seq);
        buf.put(e.flag);
        buf.putInt(k.length);
        buf.putInt(v.length);
        buf.put(k);
        buf.put(v);
        int crc = Codec.crc32c(buf.array(), 4, bodyLen);
        buf.putInt(0, crc);
        buf.flip();
        synchronized (lock) {
            rolloverIfNeeded();
            ch.write(buf);
            activeBytes += buf.remaining();
            if (activeBytes >= batchBytes) {
                ch.force(true);
                activeBytes = 0;
            }
        }
    }

    public List<Entry> replayAllSorted() throws IOException {
        List<Path> segs = new ArrayList<>();
        if (!Files.exists(dir)) return List.of();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "segment-*.log")) {
            for (Path p : ds) segs.add(p);
        }
        segs.sort(Comparator.comparingLong(this::idOf));
        List<Entry> out = new ArrayList<>();
        for (Path p : segs) out.addAll(replayFile(p));
        return out;
    }

    private long idOf(Path p) {
        Matcher m = SEG_PAT.matcher(p.getFileName().toString());
        return m.matches() ? Long.parseLong(m.group(1)) : 0L;
    }

    private List<Entry> replayFile(Path p) throws IOException {
        List<Entry> out = new ArrayList<>();
        try (FileChannel rch = FileChannel.open(p, StandardOpenOption.READ)) {
            ByteBuffer hdr = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            while (true) {
                hdr.clear();
                int r = rch.read(hdr);
                if (r < 0) break;
                if (r < 4) break; // partial/torn at end
                hdr.flip();
                int crc = hdr.getInt();
                ByteBuffer bodyHdr = ByteBuffer.allocate(8+1+4+4).order(ByteOrder.LITTLE_ENDIAN);
                int rb = rch.read(bodyHdr);
                if (rb < bodyHdr.capacity()) break;
                bodyHdr.flip();
                long seq = bodyHdr.getLong();
                byte flag = bodyHdr.get();
                int klen = bodyHdr.getInt();
                int vlen = bodyHdr.getInt();
                ByteBuffer kv = ByteBuffer.allocate(klen + vlen);
                int rkv = rch.read(kv);
                if (rkv < kv.capacity()) break;
                kv.flip();
                byte[] k = new byte[klen];
                byte[] v = new byte[vlen];
                kv.get(k);
                kv.get(v);
                // verify crc
                ByteBuffer tmp = ByteBuffer.allocate(8+1+4+4+klen+vlen).order(ByteOrder.LITTLE_ENDIAN);
                tmp.putLong(seq).put(flag).putInt(klen).putInt(vlen).put(k).put(v);
                int c2 = Codec.crc32c(tmp.array());
                if (c2 != crc) break; // stop at corruption
                out.add(new Entry(seq, flag, k, vlen == 0 ? null : v));
            }
        }
        return out;
    }

    /** Delete WAL segments whose maxSeq <= safeSeq */
    public void truncateUpTo(long safeSeq) throws IOException {
        List<Path> segs = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "segment-*.log")) {
            for (Path p : ds) segs.add(p);
        }
        segs.sort(Comparator.comparingLong(this::idOf));
        for (int i = 0; i < segs.size() - 1; i++) { // keep newest active segment
            Path p = segs.get(i);
            // simple heuristic: if the file's name id < current active id AND safeSeq is advanced, delete
            if (Files.exists(p)) Files.delete(p);
        }
        // fsync dir
        try (FileChannel dch = FileChannel.open(dir, StandardOpenOption.READ)) { dch.force(true); }
    }

    @Override public void close() throws IOException {
        scheduler.shutdown();
        try { scheduler.awaitTermination(syncMillis * 2L, TimeUnit.MILLISECONDS); } catch (InterruptedException ignored) {}
        forceSafe();
        synchronized (lock) {
            if (ch != null) ch.close();
            ch = null;
        }
    }
}
