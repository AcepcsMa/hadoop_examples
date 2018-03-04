package join;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义JavaBean, 实现WritableComparable接口
 */
public class MapJoinBean implements WritableComparable<MapJoinBean> {
	public String userID;
	public String uName;
	public String phone;
	public String orderID;
	public String price;
	public String date;
	public String flag;

	public MapJoinBean() {

	}

	public MapJoinBean(String userID, String uName, String phone, String orderID, String price, String date, String flag) {
		this.userID = userID;
		this.uName = uName;
		this.phone = phone;
		this.orderID = orderID;
		this.price = price;
		this.date = date;
		this.flag = flag;
	}

	public void set(String userID, String uName, String phone, String orderID, String price, String date, String flag) {
		this.userID = userID;
		this.uName = uName;
		this.phone = phone;
		this.orderID = orderID;
		this.price = price;
		this.date = date;
		this.flag = flag;
	}

	@Override
	public String toString() {
		return "uName='" + uName + '\'' +
				", phone='" + phone + '\'' +
				", orderID='" + orderID + '\'' +
				", price='" + price + '\'' +
				", date='" + date + '\'';
	}

	public String getUserID() {
		return userID;
	}

	public void setUserID(String userID) {
		this.userID = userID;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	public String getuName() {
		return uName;
	}

	public void setuName(String uName) {
		this.uName = uName;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getOrderID() {
		return orderID;
	}

	public void setOrderID(String orderID) {
		this.orderID = orderID;
	}

	public String getPrice() {
		return price;
	}

	public void setPrice(String price) {
		this.price = price;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(userID);
		dataOutput.writeUTF(uName);
		dataOutput.writeUTF(phone);
		dataOutput.writeUTF(orderID);
		dataOutput.writeUTF(price);
		dataOutput.writeUTF(date);
		dataOutput.writeUTF(flag);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		userID = dataInput.readUTF();
		uName = dataInput.readUTF();
		phone = dataInput.readUTF();
		orderID = dataInput.readUTF();
		price = dataInput.readUTF();
		date = dataInput.readUTF();
		flag = dataInput.readUTF();
	}

	@Override
	public int compareTo(MapJoinBean o) {
		if(o == null) {
			throw new RuntimeException();
		}
		if(userID.compareTo(o.getUserID()) == 0) {
			return orderID.compareTo(o.getOrderID());
		}
		return userID.compareTo(o.getUserID());
	}
}
