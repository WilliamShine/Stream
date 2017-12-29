package com.YunTu.ModolZookeeper;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 级联删除znode
 * @author 84031
 *
 */
public class Delete_API_Sync_Usage implements Watcher {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private static ZooKeeper zk;

    public static void main(String[] args) throws Exception {
        String path = "/zookeeper";
        zk = new ZooKeeper("47.100.9.7:2181", 5000,
                new Delete_API_Sync_Usage());
        connectedSemaphore.await();

        //创建地址
       /* zk.create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("success create znode: " + path);*/
        //创建子节点
        /*zk.create(path + "/c1", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("success create znode: " + path + "/c1");*/
        
        //删除节点
       /* try {
            zk.delete(path, -1);
        } catch (Exception e) {
            System.out.println("fail to delete znode: " + path);
        }*/
       /* List<String> childList = zk.getChildren(path, false);
        for (String string : childList) {
			System.out.println(string);
		}*/
        deleteZnode(path, zk);
        System.out.println("success delete znode: " + path);
        
       /* zk.delete(path + "/c1", -1);
        System.out.println("success delete znode: " + path + "/c1");
        zk.delete(path, -1);        
        System.out.println("success delete znode: " + path);*/

        //Thread.sleep(Integer.MAX_VALUE);
        //zk.close();
    }

    public void process(WatchedEvent event) {
        if (KeeperState.SyncConnected == event.getState()) {
            if (EventType.None == event.getType() && null == event.getPath()) {
                connectedSemaphore.countDown();
            }
        }
    }
    
    /**
     * Znode递归级联删除
     * @param path
     * @throws Exception 
     * @throws KeeperException 
     */
	private static void deleteZnode(String path, ZooKeeper zk) {
		try {
			List<String> childList = zk.getChildren(path, false);
			if (childList == null) {
				zk.delete(path, -1);
			} else {
				for (String childPath : childList) {
					deleteZnode(path+"/"+childPath, zk);
				}
				zk.delete(path, -1);
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
}