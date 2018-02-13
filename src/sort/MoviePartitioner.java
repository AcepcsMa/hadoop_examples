package sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义Partitioner, 用于划分K-V对被哪个Reducer取走
 */
public class MoviePartitioner extends Partitioner<MovieBean, DoubleWritable> {
	@Override
	public int getPartition(MovieBean movieBean, DoubleWritable doubleWritable, int numReducers) {
		return movieBean.getMovieID().hashCode() % numReducers;
	}
}
