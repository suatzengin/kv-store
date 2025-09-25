package lsmkv;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface KvStore extends Closeable {
    void put(byte[] key, byte[] value) throws Exception;
    Optional<byte[]> read(byte[] key) throws Exception;
    List<KeyValue> readKeyRange(byte[] startInclusive, byte[] endExclusive) throws Exception;
    void batchPut(List<byte[]> keys, List<byte[]> values) throws Exception;
    void delete(byte[] key) throws Exception;
    @Override void close() throws IOException;
}
