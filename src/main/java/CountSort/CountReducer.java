package CountSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, CountBean, NullWritable, Text> {
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<CountBean> values, Context context) throws java.io.IOException, InterruptedException {
        int totalSale = 0;
        int totalBrow = 0;
        for (CountBean value : values) {
            totalSale += value.getSale();
            totalBrow += value.getBrow();
        }
        outValue.set(key.toString() + "," + totalSale + "," + totalBrow);
        context.write(NullWritable.get(), outValue);
    }
}
