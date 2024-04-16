package org.apache.curator.framework.recipes.locks;

import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

/**
 * 默认锁内置辅助驱动器
 */
public class StandardLockInternalsDriver implements LockInternalsDriver {

    /** 创建锁节点：临时有序节点 */
    @Override
    public String createsTheLock(CuratorFramework client, String path, byte[] lockNodeBytes) throws Exception {
        CreateMode createMode = CreateMode.EPHEMERAL_SEQUENTIAL;
        String sequence = getSortingSequence();
        if (sequence != null) {
            path += sequence;
            createMode = CreateMode.EPHEMERAL;
        }
        String ourPath;
        if (lockNodeBytes != null) {
            ourPath = client.create()
                    .creatingParentContainersIfNeeded()
                    .withProtection()
                    .withMode(createMode)
                    .forPath(path, lockNodeBytes);
        } else {
            ourPath = client.create()
                    .creatingParentContainersIfNeeded()
                    .withProtection()
                    .withMode(createMode)
                    .forPath(path);
        }
        return ourPath;
    }

    /** 判断是否加锁成功：先获取当前子节点在所有有序子节点中的索引，判断是否为序号最小，若是则获取锁成功，否则获取失败返回其前一个节点用于后序watch监听 */
    @Override
    public PredicateResults getsTheLock(
            CuratorFramework client, List<String> children, String sequenceNodeName, int maxLeases) throws Exception {
        int ourIndex = children.indexOf(sequenceNodeName);
        validateOurIndex(sequenceNodeName, ourIndex);
        boolean getsTheLock = ourIndex < maxLeases;
        String pathToWatch = getsTheLock ? null : children.get(ourIndex - maxLeases);
        return new PredicateResults(pathToWatch, getsTheLock);
    }

    protected String getSortingSequence() {
        return null;
    }

    @Override
    public String fixForSorting(String str, String lockName) {
        return standardFixForSorting(str, lockName);
    }

    public static String standardFixForSorting(String str, String lockName) {
        int index = str.lastIndexOf(lockName);
        if (index >= 0) {
            index += lockName.length();
            return index <= str.length() ? str.substring(index) : "";
        }
        return str;
    }

    static void validateOurIndex(String sequenceNodeName, int ourIndex) throws KeeperException {
        if (ourIndex < 0) {
            throw new KeeperException.NoNodeException("Sequential path not found: " + sequenceNodeName);
        }
    }

}
