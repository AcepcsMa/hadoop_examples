package sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MovieReducer extends Reducer<MovieBean, DoubleWritable, Text, DoubleWritable> {
	@Override
	protected void reduce(MovieBean key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		for(DoubleWritable value : values) {
			context.write(key.getMovieID(), value);
		}
	}
}
