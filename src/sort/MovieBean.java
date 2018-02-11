package sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class MovieBean implements WritableComparable<MovieBean>{
	public Text movieID;
	public DoubleWritable score;

	public MovieBean() {

	}

	public MovieBean(Text movieID, DoubleWritable score) {
		this.movieID = movieID;
		this.score = score;
	}

	public void set(Text movieID, DoubleWritable score) {
		this.movieID = movieID;
		this.score = score;
	}

	public Text getMovieID() {
		return movieID;
	}

	public void setMovieID(Text movieID) {
		this.movieID = movieID;
	}

	public DoubleWritable getScore() {
		return score;
	}

	public void setScore(DoubleWritable score) {
		this.score = score;
	}

	@Override
	public String toString() {
		return "movieID=" + movieID +
				", score=" + score;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	@Override
	public int compareTo(MovieBean o) {
		if(o == null) {
			return -1;
		}
		if(this.movieID.compareTo(o.getMovieID()) == 0) {
			return -score.compareTo(o.getScore());
		}
		return this.movieID.compareTo(o.getMovieID());
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(movieID.toString());
		dataOutput.writeDouble(score.get());
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		movieID = new Text(dataInput.readUTF());
		score = new DoubleWritable(dataInput.readDouble());
	}
}
