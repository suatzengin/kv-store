package lsmkv;

import java.nio.file.Path;
import java.util.List;

public class SmokeTest {
    public static void main(String[] args) throws Exception {
        Path root = Path.of("./lsm-root");
        try (var kv = new LsmKvStore(Options.defaults(root))) {
            kv.put("a".getBytes(), "1".getBytes());
            kv.put("b".getBytes(), "2".getBytes());
            kv.delete("b".getBytes());
            kv.batchPut(List.of("c".getBytes(), "d".getBytes()), List.of("3".getBytes(), "4".getBytes()));
            System.out.println(new String(kv.read("a".getBytes()).orElse(null))); // 1
            System.out.println(kv.read("b".getBytes()).isPresent()); // false
            for (var kvp : kv.readKeyRange("a".getBytes(), "z".getBytes())) {
                System.out.println(new String(kvp.key()) + " -> " + new String(kvp.value()));
            }
        }
    }
}