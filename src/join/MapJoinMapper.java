package join;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 实现Map端join的Mapper
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, MapJoinBean, NullWritable> {

	private Map<String, List<String>> userTable = new HashMap<>();
	private MapJoinBean outputKey = new MapJoinBean();

	public static final String ORDER_FILE = "order.txt";

	/**
	 * Mapper会自行调用的一个初始化方法
	 * @param context Job信息
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		// 获取系统"投放"到本地的cache文件, 本问题中只有1个，即user.txt
		URI[] uris = context.getCacheFiles();
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream inputStream = fs.open(new Path(uris[0]));
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

		// 逐行读取user.txt中的数据, 构建userTable
		String line = null;
		while((line = reader.readLine()) != null) {
			String[] terms = line.split(",");
			if(!userTable.containsKey(terms[0])) {
				userTable.put(terms[0], new ArrayList<>());
			}
			userTable.get(terms[0]).add(terms[1]);
			userTable.get(terms[0]).add(terms[2]);
		}

		reader.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String fileName = fileSplit.getPath().getName();

		// 不需要再处理User数据文件了, 因为已经通过setup把其数据加载到内存中的userTable
		if(fileName.equals(ORDER_FILE)) {
			String line = value.toString();
			String[] terms = line.split(",");

			// 从本地的"小表" —— userTable处获得userID对应的数据
			String userName = userTable.get(terms[0]).get(0);
			String phone = userTable.get(terms[0]).get(1);

			// 不再需要flag字段, 简单置为空串
			outputKey.set(terms[0], userName, phone, terms[1], terms[2], terms[3], "");
			context.write(outputKey, NullWritable.get());
		}
	}
}
