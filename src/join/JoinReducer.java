package join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reducer类
 */
public class JoinReducer extends Reducer<Text, JoinBean, JoinBean, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {
		JoinBean userInfo = new JoinBean();
		List<JoinBean> orders = new ArrayList<>();

		for(JoinBean info : values) {
			// 同一个UserID，有且仅有1个User对象，其他均为该User的Order对象
			if(info.getFlag().equals("user")) {
				try {
					BeanUtils.copyProperties(userInfo, info);
				} catch (Exception e) {
					System.out.println(e.toString());
				}
			} else if(info.getFlag().equals("order")){
				JoinBean order = new JoinBean();
				try {
					BeanUtils.copyProperties(order, info);
				} catch (Exception e) {
					System.out.println(e.toString());
				}
				orders.add(order);
			}
		}

		// 提取所需属性, 合并, 输出
		for(JoinBean order : orders) {
			JoinBean record = new JoinBean();
			record.set(key.toString(), userInfo.getuName(), userInfo.getPhone(), order.getOrderID(), order.getPrice(), order.getDate(), order.getFlag());
			context.write(record, NullWritable.get());
		}
	}
}
