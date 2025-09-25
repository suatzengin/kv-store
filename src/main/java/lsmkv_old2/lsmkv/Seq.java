package lsmkv;

import java.util.concurrent.atomic.AtomicLong;

final class Seq {
    private final AtomicLong seq = new AtomicLong(1L);

    long next() {
        return seq.getAndIncrement();
    }

    long current() {
        return seq.get();
    }

    void jumpTo(long value) {
        seq.updateAndGet(prev -> Math.max(prev, value));
    }
}
