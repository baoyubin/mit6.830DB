package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;

import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.Iterator;
import java.util.List;



/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    private LockManager lockManager;    

    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int BufferPageNum = DEFAULT_PAGES;
    private LruCache<PageId,Page> pageCache;

    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.BufferPageNum = numPages;
        pageCache = new LruCache<PageId,Page>(numPages);
        lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
       
        long st = System.currentTimeMillis();
        boolean isacquired = false;
        while(!isacquired){

            try {
                isacquired = lockManager.requireLock(pid, tid, perm);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long now = System.currentTimeMillis();
            if(now - st > 500){
                System.out.println("[throw] " + Thread.currentThread().getName() + ": the " + pid + "  " + tid + " deadLock");
                throw new TransactionAbortedException();
            }
        }
        

        Page page = pageCache.get(pid);
        if (page == null){
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            page = file.readPage(pid);

            if (pageCache.getSize() == this.BufferPageNum)
            this.evictPage();

            pageCache.put(pid, page);     
        }


        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.isHoldLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit){
            try {
                flushPages(tid);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }else{
            restorePages(tid);
        }
        lockManager.completeTranslation(tid);
    }

    private synchronized void restorePages(TransactionId tid) {
        List<Page> map = pageCache.getPageList();
        Iterator iterator = map.iterator();
        while (iterator.hasNext()){
            Page cur = (Page)iterator.next();
            if (cur.isDirty()==tid){
                discardPage(cur.getId());
                return;
            }
        }      
    }


    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
        * Marks any pages that were dirtied by the operation as dirty by calling
        * their markDirty bit, and adds versions of any pages that have 
        * been dirtied to the cache (replacing any existing versions of those pages) so 
        * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pgs = dbFile.insertTuple(tid, t);
        for (Page page : pgs) {
           

            page.markDirty(true, tid); 
            
            //if ( pageCache.get(page.getId()) == null) throw new DbException("page donot exist in the buffbool");
            
            pageCache.put(page.getId(), page);

            if (pageCache.getSize() - 1 > this.BufferPageNum)
            this.evictPage();
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
       
        RecordId recordId = t.getRecordId();
        if (recordId == null) {
            throw new DbException("recordId is null when delete tuple");
        }
        DbFile dbFile = Database.getCatalog().getDatabaseFile(recordId.getPageId().getTableId());
        List<Page> pages = dbFile.deleteTuple(tid, t);
        for (Page page : pages) {
            if (pageCache.getSize() == this.BufferPageNum)
            this.evictPage();

            page.markDirty(true, tid);
        
            //if ( pageCache.get(page.getId()) == null) throw new DbException("page donot exist in the buffbool");

            pageCache.put(page.getId(), page);
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        List<PageId> map = pageCache.getPageIdList();
        Iterator iterator = map.iterator();
        while (iterator.hasNext()){
            PageId cur = (PageId)iterator.next();
            Page page = pageCache.get(cur);
            if (page.isDirty() == null) return;
            Database.getCatalog().getDatabaseFile(cur.getTableId()).writePage(page);
        }
       
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1

        Page page = pageCache.evictPage(pid);
        if (page == null)
        try {
            throw new DbException("pid is not exist in buffer pool");
        } catch (DbException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }    
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pageCache.get(pid);
        if (page.isDirty() == null) return;
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        List<Page> map = pageCache.getPageList();
        Iterator iterator = map.iterator();
        while (iterator.hasNext()){
            Page cur = (Page)iterator.next();
            if (cur.isDirty()==tid){
                flushPage(cur.getId());
                return;
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        
        List<Page> map = pageCache.getPageList();
        Iterator iterator = map.iterator();
        while (iterator.hasNext()){
            Page cur = (Page)iterator.next();  
            
            if ( cur.isDirty()==null){
                if (lockManager.isHoldLock(cur.getId()) != null){
                    System.out.println("==============================");
                    for (PageLock pageLock : lockManager.isHoldLock(cur.getId()).values()){
                        System.out.println(pageLock.getType());
                    }
                    System.out.println("==============================");
                }
                discardPage(cur.getId());
                return;
            }
        }
        
        throw new DbException("all pages marked dirty");
    }

}
