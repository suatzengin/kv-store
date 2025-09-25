package lsmkv_old2.lsmkv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class SstWriter implements AutoCloseable {
    private final Path tmpPath;
    private final Path finalPath;
    private final int blockSize;
    private FileChannel ch;
    private final SparseIndex index = new SparseIndex();
    private final List<byte[]> bloomKeys = new ArrayList<>();

    public SstWriter(Path dir, String fileName, int blockSize) throws IOException {
        Files.createDirectories(dir);
        this.tmpPath = dir.resolve(fileName + ".tmp");
        this.finalPath = dir.resolve(fileName);
        this.blockSize = blockSize;
        this.ch = FileChannel.open(tmpPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    public void writeAll(java.util.NavigableMap<byte[], Entry> map) throws IOException {
        // Write blocks: [crc32c|blockLen|...entries...] * N  then footer: [indexLen|index|bloomLen|bloom]
        ByteBuffer block = ByteBuffer.allocate(blockSize).order(ByteOrder.LITTLE_ENDIAN);
        long offset = 0;
        for (Map.Entry<byte[], Entry> me : map.entrySet()) {
            Entry e = me.getValue();
            // add to bloom
            bloomKeys.add(e.key);
            byte[] val = e.value == null ? new byte[0] : e.value;
            int recLen = 1 + 4 + 4 + e.key.length + val.length; // flag|klen|vlen|k|v
            if (block.position() + 4 + 4 + recLen > block.capacity()) {
                // flush block
                flushBlock(block);
                offset += block.position();
                index.add(me.getKey(), offset);
                block.clear();
            }
            block.put(e.flag);
            block.putInt(e.key.length);
            block.putInt(val.length);
            block.put(e.key);
            block.put(val);
        }
        if (block.position() > 0) {
            flushBlock(block);
            offset += block.position();
        }

        // footer
        // (we keep index minimal: keyLen|key|offset) repeated
        java.io.ByteArrayOutputStream idx = new java.io.ByteArrayOutputStream();
        for (SparseIndex.Entry ie : index.entries()) {
            idx.writeBytes(java.nio.ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(ie.key.length).array());
            idx.writeBytes(ie.key);
            idx.writeBytes(java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN).putLong(ie.offset).array());
        }
        byte[] indexBytes = idx.toByteArray();

        BloomFilter bloom = new BloomFilter(10, Math.max(1, bloomKeys.size()));
        for (byte[] k : bloomKeys) bloom.add(k);
        byte[] bloomBytes = bloom.toBytes();

        ByteBuffer footer = ByteBuffer.allocate(4 + indexBytes.length + 4 + bloomBytes.length).order(ByteOrder.LITTLE_ENDIAN);
        footer.putInt(indexBytes.length).put(indexBytes);
        footer.putInt(bloomBytes.length).put(bloomBytes);
        footer.flip();
        ch.write(footer);
    }

    private void flushBlock(ByteBuffer block) throws IOException {
        block.flip();
        byte[] body = new byte[block.remaining()];
        block.get(body);
        int crc = Codec.crc32c(body);
        ByteBuffer out = ByteBuffer.allocate(4 + 4 + body.length).order(ByteOrder.LITTLE_ENDIAN);
        out.putInt(crc);
        out.putInt(body.length);
        out.put(body);
        out.flip();
        ch.write(out);
    }

    public Path install() throws IOException {
        // fsync file, rename, fsync directory
        ch.force(true);
        ch.close();
        Files.move(tmpPath, finalPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        try (FileChannel dch = FileChannel.open(finalPath.getParent(), StandardOpenOption.READ)) {
            dch.force(true);
        }
        return finalPath;
    }

    @Override public void close() throws IOException { if (ch != null) ch.close(); }
}
