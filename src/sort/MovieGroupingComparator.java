package sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义的GroupingComparator
 */
public class MovieGroupingComparator extends WritableComparator {

	/**
	 * 构造函数, 告知自定义Bean类
	 */
	protected MovieGroupingComparator() {
		super(MovieBean.class, true);
	}

	/**
	 * 重写WritableComparator接口的compare方法(类似于普通Comparator接口)
	 * @param a movieA
	 * @param b movieB
	 * @return result of comparison
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MovieBean movieA = (MovieBean) a;
		MovieBean movieB = (MovieBean) b;
		return movieA.getMovieID().compareTo(movieB.getMovieID());
	}
}
