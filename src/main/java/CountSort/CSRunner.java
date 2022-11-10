package CountSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class CSRunner {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration(), "maxleaf");
        String countInputPath = "/output/JData_Action_201603_extra_DR/part-r-00000";
        String countOutputPath = "/output/JData_Action_201603_extra_C/";
        runCount(countInputPath, countOutputPath);
        String sortOutputPath = "/output/JData_Action_201603_extra_CS/";
        runSort(countOutputPath + "part-r-00000", sortOutputPath);
        fs.delete(new Path(countOutputPath), true);
        fs.close();
    }

    private static void runSort(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CSRunner.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(CountBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        Path output_p = new Path(outputPath);
        FileSystem fileSystem = output_p.getFileSystem(conf);
        if (fileSystem.exists(output_p))
            fileSystem.delete(output_p, true);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, output_p);

        job.waitForCompletion(true);
    }

    public static void runCount(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CSRunner.class);
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CountBean.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        Path output_p = new Path(outputPath);
        FileSystem fileSystem = output_p.getFileSystem(conf);
        if (fileSystem.exists(output_p))
            fileSystem.delete(output_p, true);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, output_p);

        job.waitForCompletion(true);
    }
}