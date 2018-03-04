package join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		Job joinJob = Job.getInstance(conf);

		// 使用Map端join的Mapper和Reducer
		joinJob.setMapperClass(MapJoinMapper.class);
		joinJob.setReducerClass(MapJoinReducer.class);

		joinJob.setJarByClass(MapJoinDriver.class);

		joinJob.setMapOutputKeyClass(MapJoinBean.class);
		joinJob.setMapOutputValueClass(NullWritable.class);
		joinJob.setOutputKeyClass(MapJoinBean.class);
		joinJob.setOutputValueClass(NullWritable.class);

		// 向各节点"投放"User数据文件
		joinJob.addCacheFile(new URI("/join/input/user.txt"));

		FileInputFormat.setInputPaths(joinJob, new Path("/join/input"));
		FileOutputFormat.setOutputPath(joinJob, new Path("/join/output"));

		System.exit(joinJob.waitForCompletion(true) ? 0 : 1);
	}
}
