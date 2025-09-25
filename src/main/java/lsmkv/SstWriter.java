package lsmkv;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SstWriter builds a single immutable SSTable file from an already-sorted
 * NavigableMap<byte[], Entry>.  The file format is:
 * <p>
 *   [ Block* ] [ Footer ]
 * <p>
 * Each Block:  [ crc32c:4 | blockLen:4 | body:blockLen bytes ]
 *      body contains repeated records:
 *          [ flag:1 | keyLen:4 | valLen:4 | key | value ]
 * <p>
 * Footer: [ indexLen:4 | index | bloomLen:4 | bloom ]
 *      index is a sparse index: repeated [keyLen:4 | key | offset:8]
 *      bloom is a serialized bloom filter of all keys.
 * <p>
 * Steps:
 *  - write blocks to a temp file
 *  - append footer with index + bloom
 *  - fsync and atomically rename temp -> final
 */
public final class SstWriter implements AutoCloseable {
    private final Path tmpPath;
    private final Path finalPath;
    private final int blockSize;
    private final FileChannel channel;
    private final SparseIndex index = new SparseIndex();
    private final List<byte[]> bloomKeys = new ArrayList<>();
    private final int bloomBitsPerKey;

    public SstWriter(Path dir, String fileName, int blockSize, int bloomBitsPerKey) throws IOException {
        this.tmpPath = dir.resolve(fileName + ".tmp");
        this.finalPath = dir.resolve(fileName);
        this.blockSize = blockSize;
        this.bloomBitsPerKey = bloomBitsPerKey;
        this.channel = FileChannel.open(tmpPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    /**
     * Write all entries from a sorted NavigableMap into SST format.
     * Blocks are filled until blockSize would overflow, then flushed.
     */
    public void writeAll(java.util.NavigableMap<byte[], Entry> map) throws IOException {
        // single in-memory buffer for accumulating a block body (not counting 8-byte header)
        ByteBuffer block = ByteBuffer.allocate(blockSize).order(ByteOrder.LITTLE_ENDIAN);
        long offset = 0;
        for (Map.Entry<byte[], Entry> mapEntry : map.entrySet()) {
            Entry entry = mapEntry.getValue();
            bloomKeys.add(entry.key()); // remember key for bloom filter

            byte[] val = entry.value() == null ? new byte[0] : entry.value();
            int recLen = 1 + 4 + 4 + entry.key().length + val.length; // flag|klen|vlen|k|v

            // if adding this record would overflow the block, flush the block to disk
            if (block.position() + 4 + 4 + recLen > block.capacity()) {
                int bodyLen = flushBlock(block);              // returns body length
                offset += 8 + bodyLen;                        // advance offset by header+body size
                // add an index entry so readers can binary-search blocks
                index.add(mapEntry.getKey(), offset);
                block.clear();                                 // reset for next block
            }

            // append record to block body
            block.put(entry.flag());
            block.putInt(entry.key().length);
            block.putInt(val.length);
            block.put(entry.key());
            block.put(val);
        }

        // flush the last block if it has data
        if (block.position() > 0) {
            flushBlock(block);
        }

        // footer
        // sparse index: repeated [keyLen|key|offset]
        ByteArrayOutputStream idx = new ByteArrayOutputStream();
        for (SparseIndex.Entry indexEntry : index.entries()) {
            idx.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(indexEntry.key().length).array());
            idx.writeBytes(indexEntry.key());
            idx.writeBytes(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(indexEntry.offset()).array());
        }
        byte[] indexBytes = idx.toByteArray();

        BloomFilter bloom = new BloomFilter(bloomBitsPerKey, Math.max(1, bloomKeys.size()));
        for (byte[] key : bloomKeys) bloom.add(key);
        byte[] bloomBytes = bloom.toBytes();

        // write footer: [indexLen][index][bloomLen][bloom]
        ByteBuffer footer = ByteBuffer.allocate(4 + indexBytes.length + 4 + bloomBytes.length).order(ByteOrder.LITTLE_ENDIAN);
        footer.putInt(indexBytes.length).put(indexBytes);
        footer.putInt(bloomBytes.length).put(bloomBytes);
        footer.flip();
        channel.write(footer);
    }

    /**
     * Flushes a full block to disk with CRC32C and length prefix.
     * Returns the body length (needed to advance offset accurately).
     */
    private int flushBlock(ByteBuffer block) throws IOException {
        block.flip();
        byte[] body = new byte[block.remaining()];
        block.get(body);

        int crc = Codec.crc32c(body);
        ByteBuffer out = ByteBuffer.allocate(4 + 4 + body.length).order(ByteOrder.LITTLE_ENDIAN);
        out.putInt(crc);            // 4-byte CRC32C of body
        out.putInt(body.length);    // 4-byte length of body
        out.put(body);              // body bytes
        out.flip();
        channel.write(out);
        return body.length;
    }

    /**
     * Finish the SST:
     *  - fsync the file contents
     *  - close the channel
     *  - atomically rename temp -> final
     *  - fsync the directory to persist the rename
     */
    public Path install() throws IOException {
        channel.force(true);     // ensure all data is on disk
        channel.close();
        Files.move(tmpPath, finalPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        // fsync directory so rename is durable
        try (FileChannel dch = FileChannel.open(finalPath.getParent(), StandardOpenOption.READ)) {
            dch.force(true);
        }
        return finalPath;
    }

    @Override
    public void close() throws IOException {
        if (channel != null) channel.close();
    }
}
