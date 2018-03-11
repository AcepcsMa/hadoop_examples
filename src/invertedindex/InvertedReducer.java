package invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 倒排索引的Reducer
 */
public class InvertedReducer extends Reducer<Text, Text, Text, DocumentBean> {

	private DocumentBean outputValue = new DocumentBean();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		outputValue.set(values);
		context.write(key, outputValue);
	}
}
