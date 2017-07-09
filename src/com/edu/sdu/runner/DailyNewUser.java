package com.edu.sdu.runner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.edu.sdu.mapper.DailyNewUserMapper;
import com.edu.sdu.reducer.DailyNewUserReducer;
import com.edu.sdu.util.Database;
import com.sdu.edu.bean.Sysmbol;
import com.sdu.edu.bean.TimeValueBean;

/**
 * 
 * @author hadoop
 *
 */
public class DailyNewUser {
	
	public static void main(String[] args) {
		Sysmbol.startDay = "2017-05-01.txt";
		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "DailyNewUser");
			job.setJarByClass(DailyNewUser.class);
			job.setMapperClass(DailyNewUserMapper.class);
			job.setReducerClass(DailyNewUserReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(TimeValueBean.class);
			FileInputFormat.addInputPath(job, new Path("/usr/local/hadoop/file/usr/" + Sysmbol.startDay));
			
			FileSystem fs2 = FileSystem.get(conf);
			Path op2 = new Path("/usr/local/hadoop/file/DailyNewUser");
			if (fs2.exists(op2)) {
				fs2.delete(op2, true);
				System.out.println("存在此输出路径，已删除！！！");
			}
			FileOutputFormat.setOutputPath(job, op2);
			job.waitForCompletion(true);
			
			/*向数据库写数据操作*/
			Database database = Database.getInstance();
			FileReader file = new FileReader("/usr/local/hadoop/file/DailyNewUser/part-r-00000");
			BufferedReader bReader = new BufferedReader(file);
			
			String str = null;
			boolean flag = false;
			while ((str = bReader.readLine()) != null) {
				String[] val = str.split("\\s+");
				flag = database.updateAppCriticalData(val[0], 
						val[1], "", "", "", "", "", val[2]);
				flag = database.updateRemainUser(val[0], val[2], val[1], "", "", "", "", "", "", "");	
			}
			System.out.println(flag);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}