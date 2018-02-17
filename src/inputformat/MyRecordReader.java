package inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 自定义的RecordReader, 用于将Split中的内容转换为自定义的K-V对形式
 */
public class MyRecordReader extends RecordReader<Text, BytesWritable> {

	private FileSplit split;		// 读入的文件split
	private boolean isFinished;		// 转换是否完成的标志位
	private Text key;				// 输出Key
	private BytesWritable value;	// 输出Value
	private JobContext context;		// 作业内容

	/**
	 * 初始化RecordReader方法
	 * @param inputSplit 读入的文件split
	 * @param taskAttemptContext 作业内容
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		this.isFinished = false;
		this.key = new Text();
		this.value = new BytesWritable();
		this.split = (FileSplit) inputSplit;
		this.context = taskAttemptContext;
	}

	/**
	 * 告诉调用者是否还有未读的下一个K-V对
	 * @return true / false
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!isFinished) {

			String fileName = this.split.getPath().getName();
			this.key.set(fileName);

			int contentLength = (int)split.getLength();
			byte[] content = new byte[contentLength];
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream inputStream = null;
			try {
				inputStream = fs.open(this.split.getPath());
				IOUtils.readFully(inputStream, content, 0, contentLength);
				value.set(content, 0, contentLength);
			} catch (Exception e) {
				System.out.println(e.toString());
			} finally {
				if(inputStream != null) {
					try {
						IOUtils.closeStream(inputStream);
					} catch (Exception e) {
						System.out.println(e.toString());
					}
				}
			}
			isFinished = true;
			return true;
		}
		return false;
	}

	/**
	 * 返回当前读到的Key
	 * @return key
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	/**
	 * 返回当前读到的Value
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return this.value;
	}

	/**
	 * 返回转换过程的状态 (是否转换完成)
	 * @return true / false
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return this.isFinished ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {

	}
}
