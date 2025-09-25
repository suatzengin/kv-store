package lsmkv;

import java.util.concurrent.atomic.AtomicLong;

public final class Seq {
    private final AtomicLong next = new AtomicLong(1);

    public long next() {
        return next.getAndIncrement();
    }

    public void setAtLeast(long value) {
        next.updateAndGet(cur -> Math.max(cur, value + 1));
    }

    public long get() {
        return next.get();
    }
}
