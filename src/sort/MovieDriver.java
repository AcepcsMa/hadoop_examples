package sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MovieDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");

		Job movieJob = Job.getInstance(conf);

		Map<Integer, Integer> map = new ConcurrentHashMap<>();

		movieJob.setJarByClass(MovieDriver.class);

		movieJob.setMapperClass(MovieMapper.class);
		movieJob.setReducerClass(MovieReducer.class);

		movieJob.setMapOutputKeyClass(MovieBean.class);
		movieJob.setMapOutputValueClass(DoubleWritable.class);
		movieJob.setOutputKeyClass(Text.class);
		movieJob.setOutputValueClass(DoubleWritable.class);

		movieJob.setGroupingComparatorClass(MovieGroupingComparator.class);
		movieJob.setPartitionerClass(MoviePartitioner.class);

		FileInputFormat.setInputPaths(movieJob, new Path("/sort/input"));
		FileOutputFormat.setOutputPath(movieJob, new Path("/sort/output"));

		System.exit(movieJob.waitForCompletion(true) ? 0 : 1);
	}
}
