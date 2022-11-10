package UserCount;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Collection;
import java.util.HashSet;

public class UCReducer extends Reducer<Text, Text, NullWritable, Text> {
    private HashSet<Text> userSet = new HashSet<Text>();
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException {
        userSet.clear();
        values.forEach(userSet::add);
        outValue.set(key.toString() + "," + userSet.size());
        context.write(NullWritable.get(), outValue);
    }
}
