package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    private int groupField;
    private Type groupType;
    private int aggField;
    private Op what;
    private final TupleDesc tupleDesc;
    private AggHander aggHander;
    
    
    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupField = gbfield;
        this.groupType = gbfieldtype;
        this.aggField = afield;
        
        this.what = what;
        
        if(groupField != Aggregator.NO_GROUPING){
            this.tupleDesc = new TupleDesc(new Type[]{groupType, Type.INT_TYPE,});
        }else{
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        }

        switch(what){
            case AVG:
                aggHander = new AvgAggHander();
                break;
            case COUNT:
                aggHander = new CountAggHander();
                break;
            case MAX:
                aggHander = new MaxAggHander();
                break;
            case MIN:
                aggHander = new MinAggHander();
                break;
            case SC_AVG:
            throw new UnsupportedOperationException("Unsupported aggregation operator ");
                //break;
            case SUM:
                aggHander = new SumAggHander();
                break;
            case SUM_COUNT:
            throw new UnsupportedOperationException("Unsupported aggregation operator ");
                //break;
            default:
            throw new UnsupportedOperationException("Unsupported aggregation operator ");
            
        }

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gField = null;
        Field aField;
        if (groupField == Aggregator.NO_GROUPING){
            aField = tup.getField(aggField);
            
        }else{
            gField = tup.getField(groupField);
            aField = tup.getField(aggField);
        }
        aggHander.hander(gField, aField);
        
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        
        for (Map.Entry<Field, Integer> entry : aggHander.gbResult.entrySet()) {
            Tuple tp = new Tuple(tupleDesc);
            
            if(groupField != Aggregator.NO_GROUPING){
                tp.setField(0, entry.getKey());
                tp.setField(1, new IntField(entry.getValue()));
            }else{
                tp.setField(0, new IntField(entry.getValue()));
            }
            tuples.add(tp);
        }

        return new TupleIterator(tupleDesc, tuples);
    }

    private static abstract class AggHander {
        Map<Field, Integer> gbResult;

        public AggHander() {
            gbResult = new HashMap<>();
        }
        public abstract void hander(Field groupField, Field field);

    }

    private class MinAggHander extends AggHander{
        
        @Override
        public void hander(Field groupField, Field field) {
            // TODO Auto-generated method stub
            int fValue = Integer.parseInt(field.toString());
            int res = gbResult.getOrDefault(groupField, Integer.MAX_VALUE);
            gbResult.put(groupField, Math.min(fValue, res));
        }
    }

    private class MaxAggHander extends AggHander{
        
        @Override
        public void hander(Field groupField, Field field) {
            // TODO Auto-generated method stub
            int fValue = Integer.parseInt(field.toString());
            int res = gbResult.getOrDefault(groupField, Integer.MIN_VALUE);
            gbResult.put(groupField, Math.max(fValue, res));
        }
    }
    private class SumAggHander extends AggHander{
       
        @Override
        public void hander(Field groupField, Field field) {
            // TODO Auto-generated method stub
            int fValue = Integer.parseInt(field.toString());
            int res = gbResult.getOrDefault(groupField, 0);
           
            gbResult.put(groupField, res+fValue);
        }
    }

    private class AvgAggHander extends AggHander{
        Map<Field, Integer> countResult;
        Map<Field, Integer> sum;
        

        public AvgAggHander() {
            this.countResult = new HashMap<>();
            this.sum = new HashMap<>();
        }


        @Override
        public void hander(Field groupField, Field field) {
            // TODO Auto-generated method stub
            int countRes = countResult.getOrDefault(groupField, 0);
            int sumRes = sum.getOrDefault(groupField, 0);

            int fValue = Integer.parseInt(field.toString());

            sumRes += fValue;
            countRes += 1;

            sum.put(groupField, sumRes);
            countResult.put(groupField, countRes);
            gbResult.put(groupField, sumRes / countRes);
        }
        
    }

    private class CountAggHander extends AggHander{

        @Override
        public void hander(Field groupField, Field field) {
            // TODO Auto-generated method stub
            int res = gbResult.getOrDefault(groupField, 0);
            gbResult.put(groupField, res+1);
        }
        
    }
     
}
