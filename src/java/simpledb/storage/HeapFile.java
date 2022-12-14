package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private TupleDesc tupleDesc;
    private File heapFile;
    private int tableId;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.tupleDesc = td;
        this.heapFile = f;
        getId();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return heapFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        tableId = heapFile.getAbsoluteFile().hashCode();
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        
        
            try {
                RandomAccessFile file = new RandomAccessFile(heapFile, "r");
                int pageSize = BufferPool.getPageSize();
                if(pid.getPageNumber() >= this.numPages() || pid.getPageNumber() < 0) {
                    file.close();
                    throw new IllegalArgumentException("the page does not exist in this file.");
                }
                file.seek(pageSize * pid.getPageNumber());
                byte[] byteBuffer = new byte[pageSize];
                file.read(byteBuffer);
                file.close();
                Page page = new HeapPage((HeapPageId)pid,byteBuffer);
                
                return page;
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return null;                   
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try {
            RandomAccessFile file = new RandomAccessFile(heapFile, "rw");
            int pageSize = BufferPool.getPageSize();
            PageId pid = page.getId();
            if(pid.getPageNumber() > this.numPages() || pid.getPageNumber() < 0) {
                file.close();
                throw new IllegalArgumentException("the page idx is beyong two or < 0.");
            }
            file.seek(pageSize * pid.getPageNumber());
            
            file.write(page.getPageData());
            file.close();
            
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(heapFile.length() / BufferPool.getPageSize() );
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage page;
        for (int pgNo = 0; ; pgNo++) {
            HeapPageId pageId = new HeapPageId(getId(), pgNo);
            try {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
                if (page.getNumEmptySlots() == 0) {
                    System.out.println("[unsafeReleasePage] : " + tid + " " + pageId);
                    Database.getBufferPool().unsafeReleasePage(tid, pageId);
                    continue;
                }
            } catch (IllegalArgumentException e) {

                page = new HeapPage(pageId, HeapPage.createEmptyPageData());  
                writePage(page);
                page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            
            }   
            break;
        }
        
        page.insertTuple(t);

        return Collections.singletonList(page);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
       
        
        HeapPage page;
        RecordId recordId = t.getRecordId();
        if (getId() != recordId.getPageId().getTableId()) {
            throw new DbException(String.format("tableId not equals %d != %d", getId(), recordId.getPageId().getTableId()));
        }
       
        page = (HeapPage) Database.getBufferPool().getPage(tid, recordId.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);

        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
        return res;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        
         return new HeapFileIterator(this, tid);
    }

}

