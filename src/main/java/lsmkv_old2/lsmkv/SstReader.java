package lsmkv;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

final class SstReader implements Closeable {
    private static final long MAGIC = 0x535354424C4B3131L;
    private final Path path;
    private final FileChannel ch;
    private final SparseIndex index;
    private final BloomFilter bloom;

    private SstReader(Path path, FileChannel ch, SparseIndex index, BloomFilter bloom) {
        this.path = path; this.ch = ch; this.index = index; this.bloom = bloom;
    }

    static SstReader open(Path p) throws IOException {
        FileChannel ch = FileChannel.open(p, StandardOpenOption.READ);
        long size = ch.size();
        ByteBuffer footer = ByteBuffer.allocate(8 * 5).order(ByteOrder.LITTLE_ENDIAN);
        ch.read(footer, size - footer.capacity());
        footer.flip();
        long indexOff = footer.getLong();
        long indexLen = footer.getLong();
        long bloomOff = footer.getLong();
        long bloomLen = footer.getLong();
        long magic = footer.getLong();
        if (magic != MAGIC) throw new IOException("Bad SST magic: " + p);
        SparseIndex idx = SparseIndex.readFrom(ch, indexOff, indexLen);
        BloomFilter bl = BloomFilter.readFrom(ch, bloomOff, (int)bloomLen);
        return new SstReader(p, ch, idx, bl);
    }

    Optional<Entry> get(byte[] key) throws IOException {
        if (!bloom.mayContain(key)) return Optional.empty();
        long off = index.seekBlockOffset(key);
        if (off < 0) return Optional.empty();
// read len varint
        ByteBuffer lenTmp = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN);
        ch.read(lenTmp, off);
        lenTmp.flip();
        int blockLen = Codec.getVarInt(lenTmp);
        int lenSize = lenTmp.position();
        ByteBuffer block = ByteBuffer.allocate(blockLen).order(ByteOrder.LITTLE_ENDIAN);
        ch.read(block, off + lenSize);
        block.flip();
// scan records in block
        while (block.hasRemaining()) {
            int klen = Codec.getVarInt(block);
            int vlen = Codec.getVarInt(block);
            long seq = block.getLong();
            byte flag = block.get();
            byte[] k = new byte[klen]; block.get(k);
            byte[] v = (flag == Entry.FLAG_PUT) ? new byte[vlen] : null;
            if (flag == Entry.FLAG_PUT) block.get(v);
            int cmp = ByteArrays.LEXICOGRAPHIC.compare(k, key);
            if (cmp == 0) return Optional.of(new Entry(seq, flag, k, v));
            if (cmp > 0) return Optional.empty(); // we passed the key in sorted order
        }
        return Optional.empty();
    }

    @Override public void close() throws IOException { ch.close(); }
}