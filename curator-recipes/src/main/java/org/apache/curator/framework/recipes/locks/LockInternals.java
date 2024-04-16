package org.apache.curator.framework.recipes.locks;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.RetryLoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * 锁内置辅助对象
 */
public class LockInternals {

    private final WatcherRemoveCuratorFramework client;
    private final String path;
    private final String basePath;
    private final LockInternalsDriver driver;
    private final String lockName;
    private final AtomicReference<RevocationSpec> revocable = new AtomicReference<RevocationSpec>(null);
    private final CuratorWatcher revocableWatcher = new CuratorWatcher() {
        @Override
        public void process(WatchedEvent event) throws Exception {
            if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                checkRevocableWatcher(event.getPath());
            }
        }
    };
    private final Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            // 锁释放，回调该watch通知唤醒等待锁的线程
            client.postSafeNotify(LockInternals.this);
        }
    };
    private volatile int maxLeases;
    static final byte[] REVOKE_MESSAGE = "__REVOKE__".getBytes();

    /** 构造方法 */
    LockInternals(CuratorFramework client, LockInternalsDriver driver, String path, String lockName, int maxLeases) {
        this.driver = driver;
        this.lockName = lockName;
        this.maxLeases = maxLeases;
        this.client = client.newWatcherRemoveCuratorFramework();
        this.basePath = PathUtils.validatePath(path);
        this.path = ZKPaths.makePath(path, lockName);
    }

    /** 尝试获取锁 */
    String attemptLock(long time, TimeUnit unit, byte[] lockNodeBytes) throws Exception {
        final long startMillis = System.currentTimeMillis();
        final Long millisToWait = (unit != null) ? unit.toMillis(time) : null;
        final byte[] localLockNodeBytes = (revocable.get() != null) ? new byte[0] : lockNodeBytes;
        int retryCount = 0;
        String ourPath = null;
        boolean hasTheLock = false;
        boolean isDone = false;
        while (!isDone) {
            isDone = true;
            try {
                // 创建加锁节点
                ourPath = driver.createsTheLock(client, path, localLockNodeBytes);
                // 自旋尝试获取锁
                hasTheLock = internalLockLoop(startMillis, millisToWait, ourPath);
            } catch (KeeperException.NoNodeException e) {
                if (client.getZookeeperClient()
                        .getRetryPolicy()
                        .allowRetry(
                                retryCount++,
                                System.currentTimeMillis() - startMillis,
                                RetryLoop.getDefaultRetrySleeper())) {
                    isDone = false;
                } else {
                    throw e;
                }
            }
        }
        if (hasTheLock) {
            return ourPath;
        }
        return null;
    }

    /** 自旋尝试获取锁 */
    private boolean internalLockLoop(long startMillis, Long millisToWait, String ourPath) throws Exception {
        boolean haveTheLock = false;
        try {
            if (revocable.get() != null) {
                client.getData().usingWatcher(revocableWatcher).forPath(ourPath);
            }
            while ((client.getState() == CuratorFrameworkState.STARTED) && !haveTheLock) {
                List<String> children = getSortedChildren();
                String sequenceNodeName = ourPath.substring(basePath.length() + 1); // +1 to include the slash
                // 判断是否获取锁成功
                PredicateResults predicateResults = driver.getsTheLock(client, children, sequenceNodeName, maxLeases);
                if (predicateResults.getsTheLock()) {
                    haveTheLock = true;
                } else {
                    // 获取锁失败，则watch监听其前一个节点，然后wait等待被唤醒
                    String previousSequencePath = basePath + "/" + predicateResults.getPathToWatch();
                    synchronized (this) {
                        try {
                            client.getData().usingWatcher(watcher).forPath(previousSequencePath);
                            if (millisToWait != null) {
                                millisToWait -= (System.currentTimeMillis() - startMillis);
                                startMillis = System.currentTimeMillis();
                                if (millisToWait <= 0) {
                                    break;
                                }
                                wait(millisToWait);
                            } else {
                                wait();
                            }
                        } catch (KeeperException.NoNodeException e) {
                            // it has been deleted (i.e. lock released). Try to acquire again
                        }
                    }
                }
            }
        } catch (Exception e) {
            ThreadUtils.checkInterrupted(e);
            deleteOurPathQuietly(ourPath, e);
            throw e;
        }
        // 若等待超时未获取到锁，则删除加锁节点
        if (!haveTheLock) {
            deleteOurPath(ourPath);
        }
        return haveTheLock;
    }

    /** 获取父节点路径下的所有子节点，并根据序号升序排序 */
    List<String> getSortedChildren() throws Exception {
        return getSortedChildren(client, basePath, lockName, driver);
    }
    public static List<String> getSortedChildren(
            CuratorFramework client, String basePath, final String lockName, final LockInternalsSorter sorter)
            throws Exception {
        try {
            List<String> children = client.getChildren().forPath(basePath);
            List<String> sortedList = Lists.newArrayList(children);
            Collections.sort(sortedList, new Comparator<String>() {
                @Override
                public int compare(String lhs, String rhs) {
                    return sorter.fixForSorting(lhs, lockName).compareTo(sorter.fixForSorting(rhs, lockName));
                }
            });
            return sortedList;
        } catch (KeeperException.NoNodeException ignore) {
            return Collections.emptyList();
        }
    }

    /** 释放锁：移除监听器、删除锁节点 */
    final void releaseLock(String lockPath) throws Exception {
        client.removeWatchers();
        revocable.set(null);
        deleteOurPath(lockPath);
    }
    private void deleteOurPath(String ourPath) throws Exception {
        try {
            client.delete().guaranteed().forPath(ourPath);
        } catch (KeeperException.NoNodeException e) {
            // ignore - already deleted (possibly expired session, etc.)
        }
    }

    synchronized void setMaxLeases(int maxLeases) {
        this.maxLeases = maxLeases;
        notifyAll();
    }

    void makeRevocable(RevocationSpec entry) {
        revocable.set(entry);
    }

    public static Collection<String> getParticipantNodes(
            CuratorFramework client, final String basePath, String lockName, LockInternalsSorter sorter)
            throws Exception {
        List<String> names = getSortedChildren(client, basePath, lockName, sorter);
        Iterable<String> transformed = Iterables.transform(names, new Function<String, String>() {
            @Override
            public String apply(String name) {
                return ZKPaths.makePath(basePath, name);
            }
        });
        return ImmutableList.copyOf(transformed);
    }

    public static List<String> getSortedChildren(
            final String lockName, final LockInternalsSorter sorter, List<String> children) {
        List<String> sortedList = Lists.newArrayList(children);
        Collections.sort(sortedList, new Comparator<String>() {
            @Override
            public int compare(String lhs, String rhs) {
                return sorter.fixForSorting(lhs, lockName).compareTo(sorter.fixForSorting(rhs, lockName));
            }
        });
        return sortedList;
    }

    private void checkRevocableWatcher(String path) throws Exception {
        RevocationSpec entry = revocable.get();
        if (entry != null) {
            try {
                byte[] bytes = client.getData().usingWatcher(revocableWatcher).forPath(path);
                if (Arrays.equals(bytes, REVOKE_MESSAGE)) {
                    entry.getExecutor().execute(entry.getRunnable());
                }
            } catch (KeeperException.NoNodeException ignore) {
                // ignore
            }
        }
    }

    private void deleteOurPathQuietly(String ourPath, Exception ex) {
        try {
            deleteOurPath(ourPath);
        } catch (Exception suppressed) {
            ex.addSuppressed(suppressed);
        }
    }

    String getLockName() {
        return lockName;
    }

    LockInternalsDriver getDriver() {
        return driver;
    }

    CuratorFramework getClient() {
        return client;
    }

    public void clean() throws Exception {
        try {
            client.delete().forPath(basePath);
        } catch (KeeperException.BadVersionException ignore) {
            // ignore - another thread/process got the lock
        } catch (KeeperException.NotEmptyException ignore) {
            // ignore - other threads/processes are waiting
        }
    }

}
