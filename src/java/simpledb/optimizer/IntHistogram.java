package simpledb.optimizer;

import java.util.Arrays;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int[] buckets;
    private int min;
    private int max;
    private double width;
    private int count;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.width = ( max - min + 1.0) / buckets;
        this.count = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if(v>=min && v<=max){
            int addValue = (int)((v - min) / width);
            buckets[addValue]++;
            count++;
        }
        
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        
        switch(op){
            case LESS_THAN:
                if(v <= min){
                    return 0.0;
                } else if (v >= max){
                    return 1.0;
                } else {
                    int index = (int)((v-min)/width);
                    double tuples = 0;
                    for(int i=0; i<index; i++){
                        tuples += buckets[i];
                    }
                    tuples += (1.0 * buckets[index] / width) * (v - (min + index * width));
                    return tuples / count;
                }
            case GREATER_THAN:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ,v);
            case EQUALS:
            if(v < min){
                return 0.0;
            } else if (v > max){
                return 0.0;
            } else {
                //double a =estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) ;
                //double b = estimateSelectivity(Predicate.Op.LESS_THAN, v);
                //return a+b;
                int index = (int) Math.floor((v - min) / width);
                double tuples = 1.0 * buckets[index] ;
                return tuples / count;
            }
            
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS,v);
            case GREATER_THAN_OR_EQ:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN,v);
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN,v) + estimateSelectivity(Predicate.Op.EQUALS,v);
            default:
                throw new UnsupportedOperationException("Operation is illegal");
        }

    	// some code goes here
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        //System.out.println(Arrays.stream(buckets).sum());
        return "IntHistogram [buckets=" + Arrays.toString(buckets) + ", min=" + min + ", max=" + max + ", width="
                + width + ", count=" + count + "]";
    }

    
}
