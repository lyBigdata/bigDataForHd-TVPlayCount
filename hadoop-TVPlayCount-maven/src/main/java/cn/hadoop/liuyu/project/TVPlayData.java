package cn.hadoop.liuyu.project;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


/*
 * 自定义TVPlayData类，继承WritableComparable,用来封装数据
 */
public class TVPlayData  implements WritableComparable<Object>{
	//直接利用java的基本数据类型int，定义成员变量daynumber、collectnumber、commentnumber、againstnumber、supportnumber
	private int daynumber;  //播放次数
	private int collectnumber;    //收藏数
    private int commentnumber;  //评论数
	private int againstnumber;  //踩数
	private int supportnumber;   //赞赏数
	
	public  TVPlayData(){
	}
	
	public  TVPlayData(int daynumber,int collectnumber, int commentnumber,int  againstnumber,int supportnumber ){
//		this.daynumber=daynumber;
//		this.collectnumber=collectnumber;
//		this.commentnumber=commentnumber;
//		this.againstnumber=againstnumber;
//		this.supportnumber=supportnumber;
		set( daynumber, collectnumber, commentnumber, againstnumber,supportnumber);
	}
	
	public void set(int daynumber,int collectnumber, int commentnumber,int  againstnumber,int supportnumber ){
		this.daynumber=daynumber;
		this.collectnumber=collectnumber;
		this.commentnumber=commentnumber;
		this.againstnumber=againstnumber;
		this.supportnumber=supportnumber;
	}
	
	
	public int getDaynumber() {
		return daynumber;
	}

	public void setDaynumber(int daynumber) {
		this.daynumber = daynumber;
	}

	public int getCollectnumber() {
		return collectnumber;
	}

	public void setCollectnumber(int collectnumber) {
		this.collectnumber = collectnumber;
	}

	public int getCommentnumber() {
		return commentnumber;
	}

	public void setCommentnumber(int commentnumber) {
		this.commentnumber = commentnumber;
	}

	public int getAgainstnumber() {
		return againstnumber;
	}

	public void setAgainstnumber(int againstnumber) {
		this.againstnumber = againstnumber;
	}

	public int getSupportnumber() {
		return supportnumber;
	}

	public void setSupportnumber(int supportnumber) {
		this.supportnumber = supportnumber;
	}

	//反序列化，实现WritableComparable的readFields()方法，以便该数据能被序列化后完成网络传输或文件输入
	public void readFields(DataInput in) throws IOException {   
		daynumber=in.readInt();
		collectnumber=in.readInt();
		commentnumber=in.readInt();
		againstnumber=in.readInt();
		supportnumber=in.readInt();
	}

	//序列化，实现WritableComparable的write()方法，以便该数据能被序列化后完成网络传输或文件输出 
	public void write(DataOutput out) throws IOException {
		out.writeInt(daynumber);
		out.writeInt(collectnumber);
		out.writeInt(commentnumber);
		out.writeInt(againstnumber);
		out.writeInt(supportnumber);
	}

	public int compareTo(Object o) {
		return 0;
	}

}
