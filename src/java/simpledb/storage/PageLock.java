package simpledb.storage;



import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;

public class PageLock {
    
    private TransactionId tid;
    private Permissions type;
    
    public PageLock(TransactionId tid, Permissions type) {
        this.tid = tid;
        this.type = type;
    }

    public Permissions getType() {
        return this.type;
    }

    public TransactionId getTid() {
        return this.tid;
    }

    public void setType(Permissions type) {
        this.type = type;
    }
}
