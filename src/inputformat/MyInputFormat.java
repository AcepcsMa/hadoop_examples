package inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 自定义InputFormat, 创建一个自定义的RecordReader(以实现自定义的读取输入文件逻辑)
 */
public class MyInputFormat extends FileInputFormat<Text, BytesWritable>{
	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit,
                                                                TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		// 初始化一个自定义的RecordReader并返回
		RecordReader<Text, BytesWritable> recordReader = new MyRecordReader();
		recordReader.initialize(inputSplit, taskAttemptContext);
		return recordReader;
	}
}
