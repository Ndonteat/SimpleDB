package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    HashMap<Field, Integer> gMap;
    HashMap<Field, Integer> number;

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
        this.gbfield = gbfield; // actually is the field number of the to-be-aggregated field in a tuple?
        this.afield = afield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        gMap = new HashMap<>();
        number = new HashMap<>();
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
        Field field;
        if (this.gbfield == NO_GROUPING){
            field = null;
        }else {
            field = tup.getField(this.gbfield);
        }

        int aggVal = 0;
        int value = Integer.parseInt(tup.getField(afield).toString());

        switch (this.what) {
            case AVG: aggVal = gMap.containsKey(field) ? gMap.get(field) + value : value;
            break;
            case MAX: aggVal = gMap.containsKey(field) ? Math.max(gMap.get(field), value) : value;
            break;
            case MIN: aggVal = gMap.containsKey(field) ? Math.min(gMap.get(field), value) : value;
            break;
            case SUM: aggVal = gMap.containsKey(field) ? gMap.get(field) + value : value;
            break;
            case COUNT: aggVal = gMap.containsKey(field) ? gMap.get(field) + 1 : 1;
            break;
            default:
                break;

        }
        gMap.put(field, aggVal);
        number.put(field, (number.containsKey(field)) ? number.get(field) + 1 : 1);

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

        Type[] typeAr;
        String[] fieldAr;

        if (gbfield != NO_GROUPING) {
            typeAr = new Type[]{gbfieldtype, Type.INT_TYPE};
            fieldAr = new String[]{"groupValue", "aggregateValue"};
        } else {
            typeAr = new Type[]{Type.INT_TYPE};
            fieldAr = new String[]{"aggregateValue"};
        }

        TupleDesc td = new TupleDesc(typeAr, fieldAr);
        List<Tuple> list = new ArrayList<>();

        if (gbfield == NO_GROUPING) {
            int aggregateValue = (what == Op.AVG) ? gMap.get(null) / number.get(null) : gMap.get(null);
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(aggregateValue));
            list.add(tuple);

            return (OpIterator) new TupleIterator(td, list);
        } else {
            for (Field groupValue : gMap.keySet()) {
                int aggregateValue = (what == Op.AVG) ? gMap.get(groupValue) / number.get(groupValue) : gMap.get(groupValue);
                Tuple tuple = new Tuple(td);
                tuple.setField(0, groupValue);
                tuple.setField(1, new IntField(aggregateValue));
                list.add(tuple);
            }

            return (OpIterator) new TupleIterator(td, list);
        }
    }

}
