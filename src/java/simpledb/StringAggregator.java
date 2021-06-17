package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    HashMap<Field, Integer> stringGMap;
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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.stringGMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbItem = gbfield == NO_GROUPING ? null : tup.getField(gbfield);

        int count = 0;
        if (stringGMap.containsKey(gbItem)) {
            count = stringGMap.get(gbItem);
        }

        if (what == Op.COUNT){
            count += 1;
        }
//        switch (what) {
//            case COUNT:
//                count += 1;
//                break;
//            default:
//                break;
//        }
        stringGMap.put(gbItem, count);
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
        Type[] typeAr;
        String[] fieldAr;

        if (gbfield == NO_GROUPING) {
            typeAr = new Type[]{Type.INT_TYPE};
            fieldAr = new String[]{"aggregateValue"};
        } else {
            typeAr = new Type[]{gbfieldtype, Type.INT_TYPE};
            fieldAr = new String[]{"groupValue", "aggregateValue"};

        }
        TupleDesc td = new TupleDesc(typeAr, fieldAr);
        List<Tuple> list = new ArrayList<>();

        if (gbfield == NO_GROUPING) {
            int aggregateValue = stringGMap.get(null);
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(aggregateValue));
            list.add(tuple);

            return (OpIterator) new TupleIterator(td, list);
        } else {
            for (Field groupValue : stringGMap.keySet()) {
                int aggregateValue = stringGMap.get(groupValue);
                Tuple tuple = new Tuple(td);
                tuple.setField(0, groupValue);
                tuple.setField(1, new IntField(aggregateValue));
                list.add(tuple);
            }

            return (OpIterator) new TupleIterator(td, list);
        }
    }

}
