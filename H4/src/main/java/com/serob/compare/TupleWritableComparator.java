package com.serob.compare;

import com.serob.main.TupleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TupleWritableComparator extends WritableComparator {
    public TupleWritableComparator() {
        super(TupleWritable.class, true);
    }

/*    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        // per your desired no-sort logic
        return 1;
    }*/

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TupleWritable tw1 = (TupleWritable)a;
        TupleWritable tw2 = (TupleWritable)b;
        return super.compare(tw1, tw2) * -1; //revers order
    }

    /*    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return super.compare(b1, s1, l1, b2, s2, l2);
    }*/
}
