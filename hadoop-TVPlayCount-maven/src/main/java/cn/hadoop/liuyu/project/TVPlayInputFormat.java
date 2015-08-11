package cn.hadoop.liuyu.project;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;


/*
 * 自定义输入数据的解析类，继承自FileInputFormat,实现对输入数据的自定义格式解析，
 * 将输入数据解析成<key,value>对，作为map()的输入
 */
public class TVPlayInputFormat extends  FileInputFormat<Text,TVPlayData>{

	@Override
	public RecordReader<Text,TVPlayData>   createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new TVPlayRecordReader();
	}
	
	 public class TVPlayRecordReader extends RecordReader<Text, TVPlayData>{

		 public LineReader in;  //行读取器
		 public Text line;   //行内容
		 public  Text key;     //声明自定义的key
		 public TVPlayData value;     //声明自定义的value
		 
		@Override
		public void close() throws IOException {
			if(in!=null)	//关闭行读取器
				in.close();
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public TVPlayData getCurrentValue() throws IOException,
				InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void initialize(InputSplit inputsplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit  split=(FileSplit)inputsplit;//获取split,FileSplit对象封装了分区的相关信息
			Configuration job=context.getConfiguration();   //获取配置文件
			Path file=split.getPath();//获取分区文件在hdfs的存储路径
			FileSystem fileSystem = file.getFileSystem(job);    //获取文件系统对象
			
			FSDataInputStream filein = fileSystem.open(file);//打开输入分区文件，作为输入流
			in=new LineReader(filein,job);  //初始化行读取器
			line=new Text();  //初始化行存储对象
			key=new Text();  //初始化key
			value=new TVPlayData();   //初始化value
			
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			int readLine = in.readLine(line);  //行读取器in.readLine()读取一行内容，存储在line中，返回读取到的字节数
			if(readLine==0)
				return false; 
			//解析每行数据
			String[] pieces=line.toString().split("\\s+");
			 if(pieces.length != 7){  
	                throw new IOException("Invalid record received");  
	         }
			 //组装自定义的解析后的key和value值
			 key.set(pieces[0]+"\t"+pieces[1]);   //key:电视剧名  \t    视频网站编号
			 value.set(Integer.parseInt(pieces[2]),Integer.parseInt(pieces[3]),Integer.parseInt(pieces[4])
					 ,Integer.parseInt(pieces[5]),Integer.parseInt(pieces[6]));
			return  true;
		}
	 }
}
