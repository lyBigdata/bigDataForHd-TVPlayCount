package cn.hadoop.liuyu.project;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 输入数据：各视频网站的数据： 电视剧名   视频网站编号   播放次数   收藏数   评论数    踩数    赞数
 * ×经过自定义的输入数据格式解析器解析后以《key为:电视剧名  \t    视频网站编号   ; value为:播放次数   收藏数   评论数    踩数    赞数》对的形式交给map()的输入进行处理
 * 后经过reduce()的处理后
 * 分别输出每个网站 每部电视剧总的统计数据
 */

public class TVPlayCount extends Configured implements Tool{
	public   static class  TVPlayMapper  extends  Mapper<Text, TVPlayData, Text, TVPlayData>{

		@Override
		protected void map(Text key, TVPlayData value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class  TVPlayReducer  extends  Reducer< Text, TVPlayData,Text,Text>{
		private  Text  rkey=new Text();  //定义reduce()输出key
		private  Text  rvalue=new Text();  //定义reduce()输出的value
		//定义支持多文件输出的实例类
		private MultipleOutputs<Text, Text>   most;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			 most = new MultipleOutputs<Text, Text>(context);   //实例化most对象，以支持多文件输出
		}
		
		@Override
		protected void reduce(Text key, Iterable<TVPlayData> values,Context arg2)
				throws IOException, InterruptedException {
			int daynumber = 0;  //定义变量，用来统计同一网站的同一电视据的播放次数
            int collectnumber = 0;//定义变量，用来统计同一网站的同一电视据的收藏次数
            int commentnumber = 0;//定义变量，用来统计同一网站的同一电视据的评论次数
            int againstnumber = 0;//定义变量，用来统计同一网站的同一电视据的踩的次数
            int supportnumber = 0;//定义变量，用来统计同一网站的同一电视据的赞的次数
            
            for(TVPlayData tvdata : values){  
            	daynumber+=tvdata.getDaynumber();
            	collectnumber+=tvdata.getCollectnumber();
            	commentnumber+=tvdata.getCommentnumber();
            	againstnumber+=tvdata.getAgainstnumber();
            	supportnumber+=tvdata.getSupportnumber();
            }
            
            //封装要输出的key,value的值  key:电视据的名字   value:所统计的相关总次数信息
            String[] TvNameAndAddress=key.toString().split("\t");
            rkey.set(TvNameAndAddress[0]);    //组装输出的key
            rvalue.set(daynumber + "\t" + collectnumber + "\t" + commentnumber
                    + "\t" + againstnumber + "\t" + supportnumber);   //组装输出value
            
            //根据视频网站的不同输出到不同的文件
            String  address=TvNameAndAddress[1].trim(); //获取视频网站的编号
            
           if("1".equals(address)){  //如果视频网站的编号是“1”，则将输出写入到 youku-r-00000的文件中
        	   most.write("youku", rkey, rvalue);
            }else if("2".equals(address)){
            	most.write("souhu", rkey, rvalue);
            }else if("3".equals(address)){
            	most.write("tudou", rkey, rvalue);
            }else if("4".equals(address)){
            	most.write("aiqiyi", rkey, rvalue);
            }else if("5".equals(address)){
            	most.write("xunlei", rkey, rvalue);
            }
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			 most.close();   //关闭多文件输出对象
		}

	}
	public int run(String[] args) throws Exception {
		Configuration conf=new Configuration();   //读取配置文件信息
		
		//获取文件系统对象，并判断输出文件路径是否存在
		Path  path=new Path(args[1]);
		FileSystem hdfs = path.getFileSystem(conf);
		if(hdfs.isDirectory(path)){
			hdfs.delete(path, true);
		}
		
		Job job=Job.getInstance(conf);  //创建作业
		job.setJarByClass(TVPlayCount.class);//设置主类
		
		FileInputFormat.addInputPath(job, new Path(args[0]));  //输入文件的路径
		FileOutputFormat.setOutputPath(job, path);   //输出文件的路径
		
		//设置Mapper相关
		job.setMapperClass(TVPlayMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TVPlayData.class);
		
		 job.setInputFormatClass(TVPlayInputFormat.class);//自定义输入格式
		 
		job.setReducerClass(TVPlayReducer.class);// 设置Reducer
        job.setOutputKeyClass(Text.class);// reduce key类型
        job.setOutputValueClass(Text.class);// reduce value类型
        
        // 自定义文件输出格式
        MultipleOutputs.addNamedOutput(job, "youku", TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "souhu", TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "tudou", TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "aiqiyi", TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "xunlei", TextOutputFormat.class,
                Text.class, Text.class);
        
        job.waitForCompletion(true);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		 int ec = ToolRunner.run(new Configuration(), new TVPlayCount(), args);
	        System.exit(ec);
	}
}
