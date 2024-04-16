package org.apache.curator.framework.recipes.locks;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.PathUtils;

/**
 * 分布式可重入排它锁：可重入、公平
 */
public class InterProcessMutex implements InterProcessLock, Revocable<InterProcessMutex> {

    /** 锁内置辅助对象、ZK父节点路径、线程数据Map（key为线程，value为该线程持有的锁数据） */
    private final LockInternals internals;
    private final String basePath;
    private final ConcurrentMap<Thread, LockData> threadData = Maps.newConcurrentMap();
    private static class LockData {
        /** 持有锁的线程、锁对应的子节点路径、锁重入次数 */
        final Thread owningThread;
        final String lockPath;
        final AtomicInteger lockCount = new AtomicInteger(1);
        private LockData(Thread owningThread, String lockPath) {
            this.owningThread = owningThread;
            this.lockPath = lockPath;
        }
    }

    /** 构造方法 */
    public InterProcessMutex(CuratorFramework client, String path) {
        this(client, path, new StandardLockInternalsDriver());
    }
    public InterProcessMutex(CuratorFramework client, String path, LockInternalsDriver driver) {
        // maxLeases写死为1,表示只有一个线程能获得资源
        this(client, path, LOCK_NAME, 1, driver);
    }
    private static final String LOCK_NAME = "lock-";
    InterProcessMutex(
            CuratorFramework client, String path, String lockName, int maxLeases, LockInternalsDriver driver) {
        basePath = PathUtils.validatePath(path);
        internals = new LockInternals(client, driver, path, lockName, maxLeases);
    }

    /** 获取锁：直到获取成功为止 */
    @Override
    public void acquire() throws Exception {
        if (!internalLock(-1, null)) {
            throw new IOException("Lost connection while trying to acquire lock: " + basePath);
        }
    }
    @Override
    public boolean acquire(long time, TimeUnit unit) throws Exception {
        return internalLock(time, unit);
    }
    private boolean internalLock(long time, TimeUnit unit) throws Exception {
        // #1 判断是否重入
        Thread currentThread = Thread.currentThread();
        LockData lockData = threadData.get(currentThread);
        if (lockData != null) {
            // re-entering
            lockData.lockCount.incrementAndGet();
            return true;
        }
        // #2 若不是重入，则尝试获取锁；若获取锁成功，则写入Map
        String lockPath = internals.attemptLock(time, unit, getLockNodeBytes());
        if (lockPath != null) {
            LockData newLockData = new LockData(currentThread, lockPath);
            threadData.put(currentThread, newLockData);
            return true;
        }
        return false;
    }

    /** 释放锁 */
    @Override
    public void release() throws Exception {
        // #1 判断是否重入
        Thread currentThread = Thread.currentThread();
        LockData lockData = threadData.get(currentThread);
        if (lockData == null) {
            throw new IllegalMonitorStateException("You do not own the lock: " + basePath);
        }
        int newLockCount = lockData.lockCount.decrementAndGet();
        if (newLockCount > 0) {
            return;
        }
        if (newLockCount < 0) {
            throw new IllegalMonitorStateException("Lock count has gone negative for lock: " + basePath);
        }
        // 若重入次数为0，则释放锁
        try {
            internals.releaseLock(lockData.lockPath);
        } finally {
            threadData.remove(currentThread);
        }
    }

    @Override
    public boolean isAcquiredInThisProcess() {
        return (threadData.size() > 0);
    }

    public Collection<String> getParticipantNodes() throws Exception {
        return LockInternals.getParticipantNodes(
                internals.getClient(), basePath, internals.getLockName(), internals.getDriver());
    }

    @Override
    public void makeRevocable(RevocationListener<InterProcessMutex> listener) {
        makeRevocable(listener, MoreExecutors.directExecutor());
    }

    @Override
    public void makeRevocable(final RevocationListener<InterProcessMutex> listener, Executor executor) {
        internals.makeRevocable(new RevocationSpec(executor, new Runnable() {
            @Override
            public void run() {
                listener.revocationRequested(InterProcessMutex.this);
            }
        }));
    }

    public boolean isOwnedByCurrentThread() {
        LockData lockData = threadData.get(Thread.currentThread());
        return (lockData != null) && (lockData.lockCount.get() > 0);
    }

    protected byte[] getLockNodeBytes() {
        return null;
    }

    protected String getLockPath() {
        LockData lockData = threadData.get(Thread.currentThread());
        return lockData != null ? lockData.lockPath : null;
    }

}
