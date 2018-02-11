package sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MovieMapper extends Mapper<LongWritable, Text, MovieBean, DoubleWritable> {

	public MovieBean outKey = new MovieBean();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] data = line.split(",");
		Double rating = Double.parseDouble(data[1]);
		outKey.set(new Text(data[0]), new DoubleWritable(rating));
		context.write(outKey, new DoubleWritable(rating));
	}
}
