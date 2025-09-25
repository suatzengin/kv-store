package lsmkv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Write-Ahead Log:
 *  - Appends records to "segment-<id>.log" in dir
 *  - Batches fsync by size (batchBytes) and by time (syncMillis)
 *  - On recovery, replays all segments in id order, stopping at torn/corrupt tail
 */
public final class Wal implements AutoCloseable {
    private final Path dir;         // WAL directory
    private final int batchBytes;   // flush after this many written bytes
    private final int syncMillis;   // periodic fsync
    private FileChannel channel;
    private long activeBytes = 0;   // bytes since last force()
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "wal-sync");
        thread.setDaemon(true);
        return thread;
    });
    private final Object lock = new Object();
    private static final Pattern SEG_PAT = Pattern.compile("segment-(\\d+)\\.log");

    public Wal(Path dir, int batchBytes, int syncMillis) throws IOException {
        this.dir = dir;
        this.batchBytes = batchBytes;
        this.syncMillis = syncMillis;
        rolloverIfNeeded();             // open next segment if none
        scheduler.scheduleAtFixedRate(this::forceSafe, syncMillis, syncMillis, TimeUnit.MILLISECONDS);
    }

    private void forceSafe() {
        try {
            synchronized (lock) {
                if (channel != null) channel.force(true);
                // fsync dir so segment creations are durable across crashes
                try (FileChannel dch = FileChannel.open(dir, StandardOpenOption.READ)) {
                    dch.force(true);
                } catch (IOException ignore) {
                }
            }
        } catch (IOException ignore) {
        }
    }

    /** Open a new segment if none is currently open. */
    private void rolloverIfNeeded() throws IOException {
        synchronized (lock) {
            if (channel == null) {
                long activeId = findMaxSegmentId() + 1;
                Path path = dir.resolve("segment-" + activeId + ".log");
                channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                activeBytes = 0;
            }
        }
    }

    /** Scan directory to find the largest existing segment id. */
    private long findMaxSegmentId() throws IOException {
        long max = 0;
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "segment-*.log")) {
            for (Path path : ds) {
                Matcher matcher = SEG_PAT.matcher(path.getFileName().toString());
                if (matcher.matches()) {
                    long id = Long.parseLong(matcher.group(1));
                    if (id > max) max = id;
                }
            }
        }
        return max;
    }

    /**
     * Append one record:
     * crc32c | seq(8) | flag(1) | keyLen(4) | valLen(4) | key | val
     * CRC covers the body (everything after the first 4 bytes)
     */
    public void append(Entry entry) throws IOException {
        // record: crc32c | seq | flag | keyLen | valLen | key | val
        byte[] key = entry.key();
        byte[] value = entry.value() == null ? new byte[0] : entry.value();
        int bodyLen = 8 + 1 + 4 + 4 + key.length + value.length;
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen).order(ByteOrder.LITTLE_ENDIAN);

        buf.position(4);    // leave space for crc32c
        buf.putLong(entry.seq());
        buf.put(entry.flag());
        buf.putInt(key.length);
        buf.putInt(value.length);
        buf.put(key);
        buf.put(value);

        int crc = Codec.crc32c(buf.array(), 4, bodyLen);
        buf.putInt(0, crc);
        buf.flip();

        synchronized (lock) {
            rolloverIfNeeded();
            channel.write(buf);
            activeBytes += buf.remaining();
            if (activeBytes >= batchBytes) {
                channel.force(true);      // size-based fsync
                activeBytes = 0;
            }
        }
    }

    /** Collect and replay all segments, oldest -> newest, stopping at torn/corrupt tails. */
    public List<Entry> replayAllSorted() throws IOException {
        List<Path> segs = new ArrayList<>();
        if (!Files.exists(dir)) return List.of();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "segment-*.log")) {
            for (Path path : ds) segs.add(path);
        }
        segs.sort(Comparator.comparingLong(this::idOf));
        List<Entry> out = new ArrayList<>();
        for (Path path : segs) out.addAll(replayFile(path));
        return out;
    }

    private long idOf(Path path) {
        Matcher matcher = SEG_PAT.matcher(path.getFileName().toString());
        return matcher.matches() ? Long.parseLong(matcher.group(1)) : 0L;
    }

    private List<Entry> replayFile(Path path) throws IOException {
        List<Entry> out = new ArrayList<>();
        try (FileChannel readChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            ByteBuffer bodyHdr = ByteBuffer.allocate(8 + 1 + 4 + 4).order(ByteOrder.LITTLE_ENDIAN);
            while (true) {
                Entry entry = readEntry(buf, bodyHdr, readChannel);
                if (entry == null) break;          // stop at torn/corrupt
                out.add(entry);
            }
        }
        return out;
    }

    private Entry readEntry(ByteBuffer buf, ByteBuffer bodyHdr, FileChannel channel) throws IOException {
        buf.clear();
        int r = channel.read(buf);
        if (r < 0) return null;
        if (r < 4) return null; // partial/torn at end
        buf.flip();
        int crc = buf.getInt();

        bodyHdr.clear();
        if (channel.read(bodyHdr) < bodyHdr.capacity()) return null;
        bodyHdr.flip();

        long seq = bodyHdr.getLong();
        byte flag = bodyHdr.get();
        int klen = bodyHdr.getInt();
        int vlen = bodyHdr.getInt();

        ByteBuffer bufKeyValue = ByteBuffer.allocate(klen + vlen);
        if (channel.read(bufKeyValue) < bufKeyValue.capacity()) return null;
        bufKeyValue.flip();

        byte[] key = new byte[klen];
        byte[] value = new byte[vlen];
        bufKeyValue.get(key);
        bufKeyValue.get(value);

        // verify crc
        ByteBuffer tmp = ByteBuffer.allocate(8 + 1 + 4 + 4 + klen + vlen).order(ByteOrder.LITTLE_ENDIAN);
        tmp.putLong(seq).put(flag).putInt(klen).putInt(vlen).put(key).put(value);
        if (Codec.crc32c(tmp.array()) != crc) return null;   // stop at corruption

        return new Entry(seq, flag, key, vlen == 0 ? null : value);
    }

    private long lastSeqInFile(Path path) throws IOException {
        long lastSeq = Long.MIN_VALUE;
        try (FileChannel rch = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer hdr = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            ByteBuffer bodyHdr = ByteBuffer.allocate(8 + 1 + 4 + 4).order(ByteOrder.LITTLE_ENDIAN);
            while (true) {
                Entry entry = readEntry(hdr, bodyHdr, rch);
                if (entry == null) break;          // stop at torn/corrupt
                lastSeq = entry.seq();             // advance last known good seq
            }
        }
        return lastSeq;
    }

    public void truncateUpTo(long safeSeq) throws IOException {
        List<Path> segs = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "segment-*.log")) {
            for (Path path : ds) segs.add(path);
        }
        if (segs.isEmpty()) return;

        segs.sort(Comparator.comparingLong(this::idOf));
        // Compute last seq per segment
        Map<Path, Long> lastSeq = new HashMap<>();
        for (Path path : segs) {
            long ls = lastSeqInFile(path);
            lastSeq.put(path, ls);
        }

        // Delete segments whose lastSeq <= safeSeq, but keep the newest segment to remain active
        for (int i = 0; i < segs.size() - 1; i++) {  // exclude newest
            Path path = segs.get(i);
            Long lSeq = lastSeq.getOrDefault(path, Long.MIN_VALUE);
            if (lSeq != Long.MIN_VALUE && lSeq <= safeSeq && Files.exists(path)) {
                Files.delete(path);
            }
        }

        // fsync dir metadata
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        }
    }

    @Override
    public void close() throws IOException {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(syncMillis * 2L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
        }
        forceSafe();
        synchronized (lock) {
            if (channel != null) channel.close();
            channel = null;
        }
    }
}
