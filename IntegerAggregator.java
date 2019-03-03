package simpledb;

import java.util.Enumeration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ConcurrentHashMap<Field, Integer> valueMap;
    private ConcurrentHashMap<Field, Integer> countMap;
    private static final Field NO_GROUNP_KEY = new IntField(0);
    
    
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
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	valueMap = new ConcurrentHashMap<Field, Integer>();
    	countMap = new ConcurrentHashMap<Field, Integer>();
    	
    	if (this.gbfield == Aggregator.NO_GROUPING) {
    		this.valueMap.put(NO_GROUNP_KEY, 0);
    		this.countMap.put(NO_GROUNP_KEY, 0);
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
    	Field key = NO_GROUNP_KEY;
    	if (gbfield != Aggregator.NO_GROUPING) {
    		key = tup.getField(this.gbfield);
    	}
    	int value = ((IntField) (tup.getField(this.afield))).getValue();
    	if (this.gbfield == Aggregator.NO_GROUPING ||
    			tup.getTupleDesc().getFieldType(this.gbfield).equals(this.gbfieldtype)) {
    		if (!(this.valueMap.containsKey(key))) {
    			this.valueMap.put(key, value);
    			this.countMap.put(key, 1);
    		} else {
	    		this.valueMap.put(key,this.combine(this.valueMap.get(key), value));
	    		this.countMap.put(key,this.countMap.get(key) + 1);
    		}
    	}
    	
    	
    }
    
    private int combine(int a, int b) {
    	switch(what) {
    	 	case MIN:
	            return Math.min(a, b);
	        case MAX:
	            return Math.max(a, b);
	        case SUM:
	        case AVG:
	            return a + b;
	        case COUNT:
	            return a + 1;
	        default:
	        	 return 0;
    	}
    	
    	
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	TupleDesc tdec;
    	Tuple t;
    	List<Tuple> tupleList = new ArrayList<Tuple>();
    	if (this.gbfield == Aggregator.NO_GROUPING) {
    		tdec = new TupleDesc(new Type[] { Type.INT_TYPE });
    		t = new Tuple(tdec);
    		int value = this.valueMap.get(NO_GROUNP_KEY);
    		if (this.what == Aggregator.Op.AVG) value /= this.countMap.get(NO_GROUNP_KEY);
    		else if (this.what == Aggregator.Op.COUNT) value = this.countMap.get(NO_GROUNP_KEY);
    		t.setField(0, new IntField(value));
    		tupleList.add(t);
    	} else {
    		tdec = new TupleDesc(new Type[] { this.gbfieldtype, Type.INT_TYPE });
	    	Enumeration<Field> keys = this.valueMap.keys();    	
	    	while (keys.hasMoreElements()) {
	    		t = new Tuple(tdec);
	    		Field key = keys.nextElement();
	    		int value = this.valueMap.get(key);
	    		if (this.what == Aggregator.Op.AVG) value /= this.countMap.get(key);
	    		else if (this.what == Aggregator.Op.COUNT) value = this.countMap.get(key);
	    		t.setField(0, key);
	    		t.setField(1, new IntField(value));
	    		tupleList.add(t);
	    	}
    	}
    	
        return new TupleIterator(tdec, tupleList);
    }
    

}
