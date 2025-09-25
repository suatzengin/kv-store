package lsmkv;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface KvStore extends Closeable {
    void put(byte[] key, byte[] value) throws IOException;

    Optional<byte[]> read(byte[] key);

    List<KeyValue> readKeyRange(byte[] startInclusive, byte[] endExclusive);

    void batchPut(List<byte[]> keys, List<byte[]> values) throws IOException;

    void delete(byte[] key) throws IOException;

    @Override
    void close() throws IOException;
}
