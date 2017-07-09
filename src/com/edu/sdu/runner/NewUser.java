package com.edu.sdu.runner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.edu.sdu.mapper.DailyNewUserMapper;
import com.edu.sdu.mapper.DedupMapper;
import com.edu.sdu.mapper.NewUserMapper;
import com.edu.sdu.reducer.DailyNewUserReducer;
import com.edu.sdu.reducer.DedupReducer;
import com.edu.sdu.reducer.NewUserReducer;
import com.edu.sdu.util.Database;
import com.sdu.edu.bean.TimeValueBean;

public class NewUser {

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "NewUser");
			job.setJarByClass(NewUser.class);
			job.setMapperClass(NewUserMapper.class);
			job.setReducerClass(NewUserReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(TimeValueBean.class);
			FileInputFormat.addInputPath(job, new Path("/usr/local/hadoop/file/nusr/13.txt"));
			
			FileSystem fs2 = FileSystem.get(conf);
			Path op2 = new Path("/usr/local/hadoop/file/NewUser");
			if (fs2.exists(op2)) {
				fs2.delete(op2, true);
				System.out.println("存在此输出路径，已删除！！！");
			}
			FileOutputFormat.setOutputPath(job, op2);
			job.waitForCompletion(true);
			
			/*向数据库写数据操作*/
			Database database = Database.getInstance();
			FileReader file = new FileReader("/usr/local/hadoop/file/NewUser/part-r-00000");
			BufferedReader bReader = new BufferedReader(file);
			String str = null;
			int index = 0;
			String one_fours = ""; String five_tens = ""; String eleven_thirtys = ""; 
			String thirty_sixtys = ""; String one_threem = ""; String four_tenm = ""; String eleven_thirtym = "";
			String thirty_sixtym = ""; String sixtym_more = "";
			String time = null;
			while ((str = bReader.readLine()) != null) {
				String val[] = str.split("\\s+");
				String app_key = val[0];
				if(time == null)
					time = val[2];
				one_fours = val[1];
				five_tens = bReader.readLine().split("\\s+")[1];
				eleven_thirtys = bReader.readLine().split("\\s+")[1];
				thirty_sixtys = bReader.readLine().split("\\s+")[1];
				one_threem = bReader.readLine().split("\\s+")[1];
				four_tenm = bReader.readLine().split("\\s+")[1];
				eleven_thirtym = bReader.readLine().split("\\s+")[1];
				thirty_sixtym = bReader.readLine().split("\\s+")[1];
				sixtym_more = bReader.readLine().split("\\s+")[1];
				
				boolean flag = database.updateNewUserData(app_key, one_fours, five_tens, eleven_thirtys, 
						thirty_sixtys, one_threem, four_tenm, eleven_thirtym, thirty_sixtym, sixtym_more, time); 
				System.out.println(flag);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
