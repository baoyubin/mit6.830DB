package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {
    private int tableid;
    private int ioCostPerPage;
    private Map<Integer, IntHistogram> integerIntHistogramMap;
    private Map<Integer, StringHistogram> stringIntHistogramMap;
    private HashMap<Integer,Integer> maxVal;
    private HashMap<Integer,Integer> minVal;
    private TupleDesc tupleDesc;
    private int pageNum;
    private int tupleNum;

    

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;

        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
        this.pageNum = ((HeapFile)dbFile).numPages();
        this.tupleDesc = dbFile.getTupleDesc();
        int tupleFieldsLen = tupleDesc.numFields();
        this.maxVal = new HashMap<>();
        this.minVal = new HashMap<>();

        this.integerIntHistogramMap = new HashMap<>();
        this.stringIntHistogramMap = new HashMap<>();

        DbFileIterator dbFileIterator= dbFile.iterator(new TransactionId());
        try {
            dbFileIterator.open();
            while (dbFileIterator.hasNext()){
                Tuple tuple = dbFileIterator.next();
                this.tupleNum++;
                for (int i=0;i < tupleFieldsLen; i++){
                    if (tupleDesc.getFieldType(i) == Type.INT_TYPE){
                        Integer fieldInteger = ((IntField) tuple.getField(i)).getValue();
                        maxVal.put(i, Integer.max(maxVal.getOrDefault(i, Integer.MIN_VALUE), fieldInteger) );
                        minVal.put(i, Integer.min(minVal.getOrDefault(i, Integer.MAX_VALUE), fieldInteger) );
                    }
                }
            }
            
        } catch (NoSuchElementException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (DbException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        for (int i = 0; i < tupleFieldsLen; i++) {
            if (tupleDesc.getFieldType(i) == Type.INT_TYPE){
                IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, minVal.get(i), maxVal.get(i));
                integerIntHistogramMap.put(i, intHistogram);
            }else{
                StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
                stringIntHistogramMap.put(i, stringHistogram);
            }
        }
        
        try {
            dbFileIterator.rewind();
            while (dbFileIterator.hasNext()){
                Tuple tuple = dbFileIterator.next();
                for (int i=0;i < tupleFieldsLen; i++){
                    if (tupleDesc.getFieldType(i) == Type.INT_TYPE){
                        IntHistogram intHistogram = integerIntHistogramMap.get(i);
                        Integer fieldInteger = ((IntField) tuple.getField(i)).getValue();
                        intHistogram.addValue(fieldInteger);
                    }else{
                        StringHistogram stringHistogram = stringIntHistogramMap.get(i);
                        String fString = ((StringField)tuple.getField(i)).getValue();
                        stringHistogram.addValue(fString);
                    }
                }
            }
            dbFileIterator.close();
        } catch (DbException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }    
            
        // for (int i = 0; i < tupleFieldsLen; i++) {
        //     if (tupleDesc.getFieldType(i) == Type.INT_TYPE){
        //         IntHistogram intHistogram = integerIntHistogramMap.get(i);
        //         System.out.print(intHistogram.toString());
        //         System.out.println();
        //     }else{
                
        //         StringHistogram stringHistogram = stringIntHistogramMap.get(i);
        //         System.out.print(stringHistogram.toString());
        //         System.out.println("==========");
        //     }
        // }
    }



    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        //may be 2.0 * pageNum * ioCostPerPage
        return 1.0 * pageNum * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (this.tupleNum * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        if (integerIntHistogramMap.containsKey(field)){
            return integerIntHistogramMap.get(field).avgSelectivity();
        }else{
            return stringIntHistogramMap.get(field).avgSelectivity();
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        
        if (integerIntHistogramMap.containsKey(field)){
            return integerIntHistogramMap.get(field).estimateSelectivity(op, ((IntField)constant).getValue() );
        }else{
            return stringIntHistogramMap.get(field).estimateSelectivity(op, ((StringField)constant).getValue() );
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return tupleNum;
    }

}
