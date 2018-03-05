import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator{

    protected SortComparator() {
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DoubleWritable key1 = (DoubleWritable) a;
        DoubleWritable key2 = (DoubleWritable) b;

        //  sorting in descending order
        int result = key1.get() < key2.get() ? 1 : key1.get() == key2.get() ? 0 : -1;
        return result;
    }



} 