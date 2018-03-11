package invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 程序入口
 */
public class InvertedDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");

		Job invertedJob = Job.getInstance(conf);

		invertedJob.setJarByClass(InvertedDriver.class);

		invertedJob.setMapperClass(InvertedMapper.class);
		invertedJob.setReducerClass(InvertedReducer.class);

		invertedJob.setMapOutputKeyClass(Text.class);
		invertedJob.setMapOutputValueClass(Text.class);
		invertedJob.setOutputKeyClass(Text.class);
		invertedJob.setOutputValueClass(DocumentBean.class);

		FileInputFormat.setInputPaths(invertedJob, new Path("/inverted_index/data/"));
		FileOutputFormat.setOutputPath(invertedJob, new Path("/inverted_index/output/"));

		System.exit(invertedJob.waitForCompletion(true) ? 1 : 0);
	}
}
