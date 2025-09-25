package lsmkv_old2.lsmkv;

import java.util.List;
import java.util.Optional;

public interface KvStore extends AutoCloseable {
    void put(byte[] key, byte[] value) throws Exception;
    void delete(byte[] key) throws Exception;
    Optional<byte[]> get(byte[] key) throws Exception;
    void batchPut(java.util.List<byte[]> keys, java.util.List<byte[]> values) throws Exception;
    java.util.List<KeyValue> readKeyRange(byte[] startInclusive, byte[] endExclusive) throws Exception;
    @Override void close() throws Exception;
}
