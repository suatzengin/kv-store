package lsmkv;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public final class Manifest {

    private final Path path;

    public Manifest(Path dir) throws IOException {
        this.path = dir.resolve("MANIFEST");
        if (!Files.exists(path)) Files.writeString(path, "", StandardCharsets.UTF_8);
    }

    public synchronized void commit(List<String> sstFiles, long lastSeq) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append("lastSeq=").append(lastSeq).append('\n');
        for (String f : sstFiles) builder.append("sst=").append(f).append('\n');
        // write temp and atomic replace
        Path tmp = path.resolveSibling("MANIFEST.tmp");
        Files.writeString(tmp, builder.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        try (FileChannel channel = FileChannel.open(path.getParent(), StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    public synchronized State load() throws IOException {
        List<String> lines = Files.readAllLines(path);
        long lastSeq = 0;
        List<String> sst = new ArrayList<>();
        for (String line : lines) {
            if (line.startsWith("lastSeq=")) lastSeq = Long.parseLong(line.substring(8));
            else if (line.startsWith("sst=")) sst.add(line.substring(4));
        }
        return new State(lastSeq, sst);
    }

    public record State(long lastSeq, List<String> sstFiles) {
    }
}
