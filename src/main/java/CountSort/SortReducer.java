package CountSort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<CountBean, Text, NullWritable, Text> {
    private Text outValue = new Text();

    @Override
    protected void reduce(CountBean key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
        for (Text value : values) {
            outValue.set(value.toString() + "," + key.getSale() + "," + key.getBrow());
            context.write(NullWritable.get(), outValue);
        }
    }
}
