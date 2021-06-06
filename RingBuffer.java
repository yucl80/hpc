
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;


public class RingBuffer<T> {
    private final long mask;

    private final int size;

    private final Object[] buffer;

    private final ContendedAtomicInteger[] messageReadStatus;

    long r1, r2, r3, r4, r5, r6, r7;
    AtomicLong readIndex = new AtomicLong(0);
    long c1, c2, c3, c4, c5, c6, c7, c8;

    final Object robj = new Object();

    final Object wobj = new Object();

    public RingBuffer(int capacity) {
        this.size = Capacity.getCapacity(capacity);
        this.mask = this.size - 1L;
        this.buffer = new Object[this.size];
        this.messageReadStatus = new ContendedAtomicInteger[this.size];
        for (int i = 0; i < this.size; i++) {
            messageReadStatus[i] = new ContendedAtomicInteger(0);
        }
    }

    public void put(final long seqId, final T data) {
        int spin = 0;
        final int putIndex = (int) (seqId & mask);
        while (true) {
            if (seqId < readIndex.get() + this.size) {
                buffer[putIndex] = data;
                messageReadStatus[putIndex].set(1);
               /* synchronized (wobj) {
                    wobj.notifyAll();
                }*/
                break;
            } else {
                LockSupport.parkNanos(1);
              /*  synchronized (robj) {
                    try {
                        robj.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }*/
            }
            // spin = Condition.progressiveYield(spin);
        }
    }

    @SuppressWarnings("unchecked")
    public T get() {
        int spin = 0;
        final int getIndex = (int) (readIndex.get() & mask);
        while (true) {
            if (messageReadStatus[getIndex].compareAndSet(1, 0)) {
                final T data = (T) buffer[getIndex];
                buffer[getIndex] = null;
                readIndex.incrementAndGet();
              /*  synchronized (robj) {
                    robj.notifyAll();
                }*/
                return data;
            } else {
                LockSupport.parkNanos(1);
                /*synchronized (wobj) {
                    try {
                        wobj.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }*/
            }
            // spin = Condition.progressiveYield(spin);
        }
    }

    @SuppressWarnings("unchecked")
    public T get(long beginTime, long msTimeout) {
        int spin = 0;
        final int getIndex = (int) (readIndex.get() & mask);
        final long expireTime = beginTime + msTimeout;
        while (true) {
            if (messageReadStatus[getIndex].compareAndSet(1, 0)) {
                final T data = (T) buffer[getIndex];
                buffer[getIndex] = null;
                readIndex.incrementAndGet();
              /*  synchronized (robj) {
                    robj.notifyAll();
                }*/
                return data;
            }

            if (System.currentTimeMillis() > expireTime) {
                return null;
            } else {
                LockSupport.parkNanos(1);
              /*synchronized (wobj) {
                    try {
                        wobj.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }*/
            }
            // spin = Condition.progressiveYield(spin);
        }
    }

    public List<T> getList(int batchSize, long msTimeout) {
        List<T> list = new ArrayList<>();
        final long beginTime = System.currentTimeMillis();
        for (int i = 0; i < batchSize; i++) {
            T data = get(beginTime, msTimeout);
            if (data != null) {
                list.add(data);
            } else {
                return list;
            }
        }

        return list;
    }

    long sumToAvoidOptimization() {
        return r1 + r2 + r3 + r4 + r5 + r6 + r7 + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + readIndex.get();
    }
}
