package lsmkv_old2.lsmkv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public final class Manifest {
    private final Path path;

    public Manifest(Path dir) throws IOException {
        Files.createDirectories(dir);
        this.path = dir.resolve("MANIFEST");
        if (!Files.exists(path)) Files.writeString(path, "", StandardCharsets.UTF_8);
    }

    public synchronized void commit(List<String> sstFiles, long lastSeq) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("lastSeq=").append(lastSeq).append('\n');
        for (String f : sstFiles) sb.append("sst=").append(f).append('\n');
        // write temp and atomic replace
        Path tmp = path.resolveSibling("MANIFEST.tmp");
        Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        try (java.nio.channels.FileChannel dch = java.nio.channels.FileChannel.open(path.getParent(), StandardOpenOption.READ)) {
            dch.force(true);
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

    public static final class State {
        public final long lastSeq;
        public final List<String> sstFiles;
        public State(long lastSeq, List<String> sstFiles) { this.lastSeq = lastSeq; this.sstFiles = sstFiles; }
    }
}
