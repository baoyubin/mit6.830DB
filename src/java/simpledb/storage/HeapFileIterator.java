package simpledb.storage;

import java.util.ArrayList;
import java.util.Iterator;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class HeapFileIterator extends AbstractDbFileIterator{
    private final HeapFile hf;
    private final TransactionId tid;
    private final int tableId;


    private int pageCur = 0;
    private Iterator<Tuple> pageIterator;
    private Boolean isOpen;

    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.hf = heapFile;
        this.tid = tid;
        this.tableId = hf.getId();
        this.isOpen = false;
        pageIterator = null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // TODO Auto-generated method stub
        if (pageCur >= hf.numPages()) {
            return;
        }

        this.isOpen = true;

        PageId pageId = new HeapPageId(tableId, pageCur);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
        pageIterator = page.iterator();

    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        // TODO Auto-generated method stub
        this.isOpen = true;

        pageCur = 0;
        PageId pageId = new HeapPageId(tableId, pageCur);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
        pageIterator = page.iterator();
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        // TODO Auto-generated method stub
        
        if(isOpen == false || pageIterator == null) return null;
        
        while(true){

            if (pageIterator.hasNext()){
                return pageIterator.next();
            } else{
                pageCur += 1;
                if (pageCur >= hf.numPages()) {
                    return null;
                }
                PageId pageId = new HeapPageId(tableId, pageCur);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                pageIterator = page.iterator();
                if(pageIterator.hasNext()) 
                    return pageIterator.next();
                
            }
            
        }
        
       
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        super.close();
        this.isOpen = false;
        pageIterator = null;
    }
}
