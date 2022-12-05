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
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
    private int groupField;
    private Type groupType;
    private int aggField;

    private final TupleDesc tupleDesc;
    private final Map<Field, Integer> map = new LinkedHashMap<>();
    

    private static final Aggregator.Op what = Aggregator.Op.COUNT;

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (!what.toString().equals(this.what.toString())) throw new IllegalArgumentException("StringAgg only supports COUNT");
        this.groupType = gbfieldtype;
        this.groupField = gbfield;
        this.aggField = afield;

        if (groupField != Aggregator.NO_GROUPING) {
            this.tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        } else {
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
       
        if (groupField == Aggregator.NO_GROUPING){
            map.put(null, map.getOrDefault(null,0)+1);
        }else{
            Field field = tup.getField(groupField);
            map.put(field, map.getOrDefault(field,0)+1);
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        
        for (Map.Entry<Field, Integer> entry : map.entrySet()) {
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

}
