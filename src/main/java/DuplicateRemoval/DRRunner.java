package DuplicateRemoval;

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

public class DRRunner {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration(), "maxleaf");
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/input"), true);
        while (files.hasNext()){
            LocatedFileStatus fileStatus = files.next();
            String fileName = fileStatus.getPath().getName();
            String outputFileName = fileName.substring(0, fileName.lastIndexOf(".")) + "_DR";
            String inputPath = "/input/" + fileName;
            String outputPath = "/output/" + outputFileName;
            runDR(inputPath, outputPath);
        }
        fs.close();
    }

    public static void runDR(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(DuplicateRemoval.DRRunner.class);
        job.setMapperClass(DRMapper.class);
        job.setReducerClass(DRReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path output_p = new Path(outputPath);
        FileSystem fileSystem = output_p.getFileSystem(conf);
        if (fileSystem.exists(output_p))
            fileSystem.delete(output_p, true);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, output_p);

        job.waitForCompletion(true);
    }
}