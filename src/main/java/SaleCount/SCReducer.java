package SaleCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.HashSet;

public class SCReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws java.io.IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values)
            sum += value.get();
        outValue.set(key.toString() + "," + sum);
        context.write(NullWritable.get(), outValue);
    }
}
