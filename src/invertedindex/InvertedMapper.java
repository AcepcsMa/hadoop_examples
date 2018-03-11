package invertedindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 倒排索引的Mapper
 */
public class InvertedMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		FileSplit split = (FileSplit) context.getInputSplit();
		String fileName = split.getPath().getName();
		outputValue.set(fileName);

		String line = value.toString();
		String[] terms = line.split(" ");
		for(String term : terms) {
			outputKey.set(term);
			context.write(outputKey, outputValue);
		}
	}
}
