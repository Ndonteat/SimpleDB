package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    ArrayList<TDItem> listItem;       //define an array of Fileds to be used in Tuple Desc constructors
    // reason to use ArrayList: easy to use the iterater(), easy to combine 2 TupleDesc(auto expand length).
    Hashtable<String, Integer> nameToIndex; //this Hashtable is used to store Fields' Name ->Index pair, easy to look up

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
        return this.listItem.iterator(); //use the iterator of ArrayList
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
        if (typeAr.length < 1){
            throw new IllegalArgumentException("typeAr must contain at least 1 entry!");
            //according to the specs, typeAr shouldn't be null.
        }
        for (Type t: typeAr) {
            if (t == null){
                throw new IllegalArgumentException("typeAr contains null value! Fix it!");
            }
        }


        int len = fieldAr.length;
        listItem = new ArrayList<TDItem>(len); //Create an empty TupleDesc with typeAr.length fields
        nameToIndex = new Hashtable<>(); // instantiate hashtable

        for (int i = 0; i < len; i++){
            listItem.add(new TDItem(typeAr[i], fieldAr[i])); //fill the empty TupleDesc with argument value passed
            if (fieldAr[i] == null){
                nameToIndex.put("isNull", i); // store Name -> Index pair into the hashtable for further use
            }
            else {
                nameToIndex.put(fieldAr[i], i); // store Name -> Index pair into the hashtable for further use
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
        if (typeAr.length < 1){
            throw new IllegalArgumentException("typeAr must contain at least 1 entry!");
        }

        int len = typeAr.length;
        listItem = new ArrayList<TDItem>(len);
        nameToIndex = new Hashtable<>();

        for (int i = 0; i < len; i++){
            listItem.add(new TDItem(typeAr[i], null)); //only difference is here use null to represent "anonymous"
            nameToIndex.put("nothing", i);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.listItem.size();
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
        String ithName = this.listItem.get(i).fieldName;
        return ithName;
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
        Type ithType = this.listItem.get(i).fieldType;
        return ithType;
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
        // some code goes
        if (name == null){
            throw new NoSuchElementException("no field with a matching name is found.");
        }
        if (nameToIndex.get(name) == null){
            throw new NoSuchElementException("no field with a matching name is found.");
        }
        int ind = nameToIndex.get(name);

        return ind;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (int i = 0; i < this.listItem.size(); i++){
            size = size + this.getFieldType(i).getLen();
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
        int len = td1.listItem.size() + td2.listItem.size();
        Type[] typeAr = new Type[len];

        String[] fieldAr = new String[len];

        for (int i = 0; i < td1.listItem.size(); i++){
            typeAr[i] = td1.listItem.get(i).fieldType;
            fieldAr[i] = td1.listItem.get(i).fieldName;
        }

        for (int i = td1.listItem.size(); i < len; i++){
            typeAr[i] = td2.listItem.get(i - td1.listItem.size()).fieldType;
            fieldAr[i] = td2.listItem.get(i - td1.listItem.size()).fieldName;
        }

        return new TupleDesc(typeAr, fieldAr);
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

        if (o == null){
            return false;
        }
        if (o.getClass() == this.getClass()){
            TupleDesc temp = (TupleDesc) o;
            if (temp.listItem.size() != this.listItem.size()){
                return false;
            }
            for (int i = 0; i < this.listItem.size(); i++){
                if (temp.getFieldType(i) != this.getFieldType(i)){
                    return false;
                }
            }
            return true;
        }else {
            return false;
        }
    }


    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
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
        String s = "";
        for (int i = 0; i < this.listItem.size(); i++){
            s = s + this.getFieldType(i) + "(" + this.getFieldName(i) + ")";
            if (i + 1 != this.listItem.size()){
                s = s + ",";
            }
        }
        return s;
    }
}
