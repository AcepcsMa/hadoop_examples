package inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * 程序入口
 */
public class Driver {

	private static final String HDFS_HOST = "hdfs://localhost:9000";
	private static final int NUM_REDUCE_TASKS = 1;
	private static final String FILE_INPUT_PATH = "/small_files/data";
	private static final String RESULT_OUTPUT_PATH = "/small_files/output";

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", HDFS_HOST);
		Job myJob = Job.getInstance(conf);

		// 通过类名设置运行的jar包
		myJob.setJarByClass(Driver.class);

		// 设置Mapper Class与Reducer Class
		myJob.setMapperClass(MyMapper.class);
		myJob.setReducerClass(MyReducer.class);

		// 设置Mapper与Reducer的输出数据类型
		myJob.setMapOutputKeyClass(Text.class);
		myJob.setMapOutputValueClass(BytesWritable.class);
		myJob.setOutputKeyClass(Text.class);
		myJob.setOutputValueClass(BytesWritable.class);

		// 设置InputFormat与OutputFormat
		// 使用自定义的InputFormat作为Input格式, Sequence文件作为Output格式
		myJob.setInputFormatClass(MyInputFormat.class);
		myJob.setOutputFormatClass(SequenceFileOutputFormat.class);

		// 自定义Reduce Task数量, 决定最终输出多少个结果文件
		myJob.setNumReduceTasks(NUM_REDUCE_TASKS);

		// 设置数据所在目录, 结果输出目录
		FileInputFormat.setInputPaths(myJob, new Path(FILE_INPUT_PATH));
		FileOutputFormat.setOutputPath(myJob, new Path(RESULT_OUTPUT_PATH));

		System.exit(myJob.waitForCompletion(true) ? 1 : 0);

	}
}
