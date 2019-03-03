package simpledb;

import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private ConcurrentHashMap<Field, Integer> valueMap;
    
    
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
    	if (what != Aggregator.Op.COUNT) throw new 
    	IllegalArgumentException("Illegal Operation");
    	
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.valueMap = new ConcurrentHashMap<Field, Integer>();
    	
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if (tup.getTupleDesc().getFieldType(this.gbfield).equals(this.gbfieldtype)) {
    		Field key = tup.getField(this.gbfield);
    		if (!this.valueMap.containsKey(key)) this.valueMap.put(key, 0);
    		this.valueMap.put(key, this.valueMap.get(key)+1);
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        TupleDesc tdec = new TupleDesc(new Type[] {this.gbfieldtype, Type.INT_TYPE});
        List<Tuple> TupleList = new ArrayList<Tuple>();
        Enumeration<Field> keys = this.valueMap.keys();
        while(keys.hasMoreElements()) {
        	Tuple t  = new Tuple(tdec);
        	Field key = keys.nextElement();
        	t.setField(0, key);
        	t.setField(1, new IntField(this.valueMap.get(key)));
        	TupleList.add(t);
        }
        return new TupleIterator(tdec, TupleList);
    }

}
