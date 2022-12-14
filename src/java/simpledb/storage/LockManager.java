package simpledb.storage;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class LockManager {
    private ConcurrentMap<PageId, ConcurrentMap<TransactionId, PageLock>> pageLocks;


    public LockManager() {
        pageLocks = new ConcurrentHashMap<>();
    }

    public synchronized boolean requireLock(PageId pid, TransactionId tid, Permissions requireType) throws TransactionAbortedException, InterruptedException {
        final String thread = Thread.currentThread().getName();
        
        if (pageLocks.size() == 0 || pageLocks.get(pid) == null){
            /*switch(requireType){
                case READ_ONLY:
                return LockShared(pid, tid);
                case  READ_WRITE:
                return LockExclusive(pid, tid);
            }*/
            //if (pageLocks.size() == 0)
            //System.out.println("[accept] " + thread + ": the LockManger have not a lock ,  the curPage" + pid + "," + tid + " require " + requireType );
            //else if (pageLocks.get(pid) == null)
            //System.out.println("[accept] " + thread + ": the curPage " + pid + " have not a lock" + tid + " require " + requireType);

            PageLock pageLock = new PageLock(tid, requireType);
            ConcurrentHashMap<TransactionId, PageLock> curPageLock = new ConcurrentHashMap<>();
            curPageLock.put(tid, pageLock);
            pageLocks.put(pid, curPageLock);
            return true;
        }



        ConcurrentMap<TransactionId, PageLock> curPageLock = pageLocks.get(pid);

        PageLock pageLock = curPageLock.get(tid);
        
        if (pageLock == null){
            if (curPageLock.size() > 1){
                switch(requireType){
                    case READ_ONLY:
                        //System.out.println("[accept] " + thread + ": the " + pid + " have many read lock，transaction" + tid + " require share lock");
                        curPageLock.put(tid, new PageLock(tid, requireType));
                        return true;
                    case READ_WRITE:
                       //System.out.println("[wait] " + thread + ": the " + pid + " have many read lock，transaction" + tid + " require READ_WRITE lock");
                       wait(20);
                       return false;
                }
            }else if(curPageLock.size() == 1){
                PageLock otherTidLock = null;
                for (PageLock i : curPageLock.values()) otherTidLock = i;
                switch(otherTidLock.getType()){
                    case READ_ONLY:
                        if (requireType == Permissions.READ_ONLY){
                            //System.out.println("[accept] " + thread + ": the " + pid + " have a read lock by other transaction，transaction" + tid + " require share lock");
                            curPageLock.put(tid, new PageLock(tid, requireType));
                            return true;
                        }else{
                            //System.out.println("[wait] " + thread + ": the " + pid + " have a read lock by " + otherTidLock.getTid() + " curTransaction" + tid + " require a write lock");
                            wait(10);
                            return false;
                        }
                    case READ_WRITE:
                        //System.out.println("[wait] " + thread + ": the " + pid + " have a READ_WRITE lock by " + otherTidLock.getTid() + " curTransaction" + tid + " require " + requireType);
                        wait(20);
                        return false;
                }
            }


        }else if (pageLock != null){
            switch(requireType){
                case READ_ONLY:
                    //System.out.println("[accept] " +  thread + ": the " + pid + " have a lock by " + tid + " require share lock");
                    return true;
                case READ_WRITE:
                    if (pageLock.getType() == Permissions.READ_ONLY){
                        if (curPageLock.size() == 1){
                            //System.out.println("[lockUpgrade] " + thread + ": the " + pid + " have a read lock by " + tid + " require write lock");
                            //return LockUpgrade(pid, tid);
                            pageLock = new PageLock(tid, requireType);
                            curPageLock.put(tid, pageLock);
                            return true;
                        }else{
                            
                            TransactionId waitTid = tid;
                            for (TransactionId tt : curPageLock.keySet()){
                                if (tt.getId() < waitTid.getId()){
                                    waitTid = tt;
                                }
                            }
                            if ( waitTid.equals(tid)) {
                                //System.out.println("[[wait]/throw] " + thread + ": the " + pid + " have many read locks " + tid + " require write lock"); 
                                wait(10);
                                return false;
                            }
                            else
                            System.out.println("[wait/[throw]] " + thread + ": the " + pid + " have many read locks " + tid + " require write lock"); 
                            throw new TransactionAbortedException();
                        }     
                    }else if (pageLock.getType() == Permissions.READ_WRITE) {
                        //System.out.println("[accept] " + thread + ": the " + pid + " have a write lock by " + tid + " require write lock");
                        return true;
                    }
            }
        }

        //System.out.println("Error:====================");
        //System.out.println(thread + ": the " + tid + " have a " + pageLock.getType() + " lock " +" require a " +  requireType + " lock");
        //System.out.println("====================");
        return false;
    }

    

    public synchronized boolean releaseLock(TransactionId tid, PageId pageId) {
        // 判断是否持有锁
        if (isHoldLock(tid, pageId)) {
            ConcurrentMap<TransactionId, PageLock> locks = pageLocks.get(pageId);
            locks.remove(tid);
            if (locks.size() == 0) {
                pageLocks.remove(pageId);
            }
            return true;
        }
        return false;
    }

    public synchronized boolean isHoldLock(TransactionId tid, PageId pageId) {
        ConcurrentMap<TransactionId, PageLock> locks = pageLocks.get(pageId);
        if (locks == null) {
            return false;
        }
        PageLock pageLock = locks.get(tid);
        if (pageLock == null) {
            return false;
        }
        return true;
    }

    public synchronized void completeTranslation(TransactionId tid) {
        // 遍历所有的页，如果对应事务持有锁就会释放
        for (PageId pageId : pageLocks.keySet()) {
            //System.out.println(tid + " releaseLock in " + pageId);
            releaseLock(tid, pageId);
        }
    }
    
    //当前页面是否有锁
    public synchronized ConcurrentMap<TransactionId, PageLock> isHoldLock(PageId pageId) {
        return pageLocks.get(pageId);
    }

}
