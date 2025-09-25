package lsmkv_old2.lsmkv;

import java.util.concurrent.atomic.AtomicLong;

public final class Seq {
    private final AtomicLong next = new AtomicLong(1);
    public long next() { return next.getAndIncrement(); }
    public void setAtLeast(long v) { next.updateAndGet(cur -> Math.max(cur, v + 1)); }
    public long get() { return next.get(); }
}
