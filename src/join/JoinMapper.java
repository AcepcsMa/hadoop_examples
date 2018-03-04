package join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

/**
 * Mapper类
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, JoinBean> {

	public static final String USER = "user";
	public static final String ORDER = "order";


	public JoinBean info = new JoinBean();
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String fileName = fileSplit.getPath().getName();	// 获取数据所属文件名

		String line = value.toString();
		String[] data = line.split(",");
		String userID = data[0];

		// 通过flag标识实例对象是User还是Order
		if(fileName.startsWith("user")) {
			info.set("", data[1], data[2], "", "", "", USER);
		} else {
			info.set("", "", "", data[1], data[2], data[3], ORDER);
		}
		context.write(new Text(userID), info);
	}
}
