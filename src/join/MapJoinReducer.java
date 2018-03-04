package join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Map端Join的Reducer
 */
public class MapJoinReducer extends Reducer<MapJoinBean, NullWritable, MapJoinBean, NullWritable> {

	@Override
	protected void reduce(MapJoinBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
}
