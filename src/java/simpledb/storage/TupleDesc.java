package simpledb.storage;

import simpledb.common.Type;

//import static org.junit.Assume.assumeTrue;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private ArrayList<TDItem> tupleDescItemList;



    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tupleDescItemList.iterator();
    }

    
    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        int len = typeAr.length;
        tupleDescItemList = new ArrayList<>();
        if (fieldAr == null){
            for (int i = 0; i < len; i++){
                TDItem tItem = new TDItem(typeAr[i], "unnamed");
                tupleDescItemList.add(tItem);
            }
            return;
        }else{
            //assumeTrue(len == fieldAr.length);
            
            for (int i = 0; i < len; i++){
            TDItem tItem = new TDItem(typeAr[i], fieldAr[i]);
            tupleDescItemList.add(tItem);
            }
        }    
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, null);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.tupleDescItemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i<0 || i>= numFields()){
            throw new NoSuchElementException();
        }

        return tupleDescItemList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i<0 || i>= numFields()){
            throw new NoSuchElementException();
        }

        return tupleDescItemList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        int len = tupleDescItemList.size();
        for (int i=0; i < len; i++){
            if (tupleDescItemList.get(i).fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (int i = 0; i < tupleDescItemList.size(); i++){
            size += tupleDescItemList.get(i).fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here    
        int lenTd1 = td1.numFields();
        int lenTd2 = td2.numFields();

        Type[] typeAr = new Type[lenTd1+lenTd2];
        String[] fieldAr = new String[lenTd1+lenTd2];

        for (int i = 0; i < lenTd1; i++){
            typeAr[i] = td1.getFieldType(i);
            fieldAr[i] = td1.getFieldName(i);
        }

        for (int i = 0; i < lenTd2; i++){
            typeAr[lenTd1+i] = td2.getFieldType(i);
            fieldAr[lenTd1+i] = td2.getFieldName(i);
        }

        return new TupleDesc(typeAr,fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o == this) return true;
        if (o == null || o.getClass() != getClass() ) return false;
        TupleDesc td = (TupleDesc)o;
        if (td.numFields() != this.numFields()) return false;
        for (int i = 0; i < td.numFields(); i++){
            if(td.getFieldType(i) != this.getFieldType(i)) return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("tupdsc hashcode() unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder stringBuilder = new StringBuilder(); 
        int len = tupleDescItemList.size();
        for (int column = 0; column < len; column++){
            stringBuilder.append(tupleDescItemList.get(column).fieldName).append("(")
            .append(tupleDescItemList.get(column).fieldType)
            .append(")");
            if(column != len - 1) stringBuilder.append(",");
        }
        return stringBuilder.toString();

    }
}
