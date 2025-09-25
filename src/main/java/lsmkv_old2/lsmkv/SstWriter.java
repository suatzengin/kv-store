package lsmkv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;

final class SstWriter {
    private static final long MAGIC = 0x535354424C4B3131L; // 'SSTBLK11'

    static void write(Path file,
                      Iterator<Map.Entry<byte[], Entry>> it,
                      int estimatedKeys,
                      Options opt) throws IOException {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            BloomFilter bloom = new BloomFilter(estimatedKeys, opt.bloomBitsPerKey);
            SparseIndex index = new SparseIndex();
            ByteBuffer block = ByteBuffer.allocate(opt.sstBlockSizeBytes);
            block.order(ByteOrder.LITTLE_ENDIAN);

            long filePos = 0;
            byte[] firstKeyOfBlock = null;

            while (it.hasNext()) {
                Map.Entry<byte[], Entry> me = it.next();
                Entry e = me.getValue();
// Try to append record to block
                int needed = // klen varint + vlen varint + seq(8) + flag(1) + key + val
                        Codec.sizeofVarInt(e.key.length) + Codec.sizeofVarInt(e.flag == Entry.FLAG_PUT ? e.value.length : 0)
                                + 8 + 1 + e.key.length + (e.flag == Entry.FLAG_PUT ? e.value.length : 0);
                if (block.position() == 0) {
                    firstKeyOfBlock = e.key;
                }
                if (needed > block.remaining()) {
// flush block as [blockLenVarint][blockBytes]
                    writeBlock(ch, block, index, filePos, firstKeyOfBlock);
                    filePos = ch.position();
                    block.clear();
                    firstKeyOfBlock = e.key;
                }
// add to bloom
                bloom.put(e.key);
// write record
                Codec.putVarInt(block, e.key.length);
                Codec.putVarInt(block, e.flag == Entry.FLAG_PUT ? e.value.length : 0);
                block.putLong(e.seq);
                block.put(e.flag);
                block.put(e.key);
                if (e.flag == Entry.FLAG_PUT) block.put(e.value);
            }
            if (block.position() > 0) {
                writeBlock(ch, block, index, filePos, firstKeyOfBlock);
                filePos = ch.position();
            }

// serialize index
            long indexOffset = filePos;
            index.writeTo(ch);
            long indexLen = ch.position() - indexOffset;

// serialize bloom
            long bloomOffset = ch.position();
            bloom.writeTo(ch);
            long bloomLen = ch.position() - bloomOffset;

// footer
            ByteBuffer footer = ByteBuffer.allocate(8 + 8 + 8 + 8 + 8).order(ByteOrder.LITTLE_ENDIAN);
            footer.putLong(indexOffset).putLong(indexLen).putLong(bloomOffset).putLong(bloomLen).putLong(MAGIC);
            footer.flip();
            ch.write(footer);
            ch.force(true);
        }
    }

    private static void writeBlock(FileChannel ch, ByteBuffer block, SparseIndex index, long filePos, byte[] firstKey) throws IOException {
        block.flip();
        int blockSize = block.remaining();
// length prefix
        ByteBuffer lenBuf = ByteBuffer.allocate(Codec.sizeofVarInt(blockSize)).order(ByteOrder.LITTLE_ENDIAN);
        Codec.putVarInt(lenBuf, blockSize);
        lenBuf.flip();
        long start = ch.position();
        ch.write(lenBuf);
        ch.write(block);
        long end = ch.position();
        index.add(firstKey, start); // store offset where length varint starts
    }
}
