package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    TupleDesc tdSchema;
    ArrayList<Field> attributeFields;
    Field[] attributes;
    RecordId id;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        if (td.numFields() < 1){
            throw new IllegalArgumentException("td must contain at least 1 entry!");
        }

        this.tdSchema = td;
        //this.attributeFields = new ArrayList<Field>(td.numFields());
        this.attributes = new Field[td.numFields()];

    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tdSchema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.id;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.id = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        /*if (i < 0 || i > this.attributeFields.size()){
            throw new IndexOutOfBoundsException("Please check index range! Must be a valid index.");
        }*/
        //this.attributeFields.set(i, f);

            this.attributes[i] = f;
            //this.attributeFields.add(i, f); // something might be wrong here, because used add instead of set


    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if (i < 0 || i >= this.attributes.length){
            System.out.println("Size: " + this.attributeFields.size());
            System.out.println("i: " + i);
            throw new IndexOutOfBoundsException("Must be a valid index.");
        }
        return this.attributes[i];
        //return this.attributeFields.get(i); //get the field according to field number
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < this.tdSchema.numFields(); i++){
            s.append(getField(i).toString());
            if (i + 1 != this.tdSchema.numFields()){
                s.append("\t\t");
            }
        }
        return s.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        ArrayList<Field> fieldList = new ArrayList<Field>();
        Collections.addAll(fieldList, attributes);
        return fieldList.iterator();

        //return this.attributeFields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        this.tdSchema = td;
        //this.attributeFields = new ArrayList<Field>(td.numFields());
    }
}
