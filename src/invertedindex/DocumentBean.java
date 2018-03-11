package invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 集成多个DocumentID的Bean
 */
public class DocumentBean implements Writable {

	private int documentCount;
	private List<String> documentIDs;

	public DocumentBean() {
		documentCount = 0;
		documentIDs = new ArrayList<>();
	}

	public void set(Iterable<Text> documentIDs) {
		this.documentIDs.clear();
		for(Text documentID : documentIDs) {
			this.documentIDs.add(documentID.toString());
		}
		documentCount = this.documentIDs.size();
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(documentCount);
		for(String documentID : documentIDs) {
			dataOutput.writeUTF(documentID);
		}
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		documentCount = dataInput.readInt();
		for(int i = 0;i < documentCount;i++) {
			documentIDs.add(dataInput.readUTF());
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(String documentID : documentIDs) {
			sb.append(documentID).append(", ");
		}
		return sb.substring(0, sb.length() - 2);
	}
}
