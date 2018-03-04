package join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class JoinDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		Job joinJob = Job.getInstance(conf);

		joinJob.setMapperClass(JoinMapper.class);
		joinJob.setReducerClass(JoinReducer.class);

		joinJob.setJarByClass(JoinDriver.class);

		joinJob.setMapOutputKeyClass(Text.class);
		joinJob.setMapOutputValueClass(JoinBean.class);
		joinJob.setOutputKeyClass(JoinBean.class);
		joinJob.setOutputValueClass(NullWritable.class);

		joinJob.addCacheFile(new URI("/join/input/user.txt"));

		FileInputFormat.setInputPaths(joinJob, new Path("/join/input"));
		FileOutputFormat.setOutputPath(joinJob, new Path("/join/output"));

		System.exit(joinJob.waitForCompletion(true) ? 0 : 1);
	}
}
