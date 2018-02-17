package inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper, 负责处理从Split中读取的K-V型数据
 */
public class MyMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {

	@Override
	protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}
}
