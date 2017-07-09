package com.edu.sdu.runner;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.edu.sdu.mapper.ActiveDevicePlayDayMapper;
import com.edu.sdu.reducer.ActiveDevicePlayDayReducer;
import com.edu.sdu.util.Database;
import com.sdu.edu.bean.Sysmbol;

public class ActiveDevicePlayDay {
	public static void main(String[] args) {
		Sysmbol.startDay = "2017-05-01.txt";
		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "ActiveDevicePlayDay");
			job.setJarByClass(ActiveDevicePlayDay.class);
			job.setMapperClass(ActiveDevicePlayDayMapper.class);
			job.setReducerClass(ActiveDevicePlayDayReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("/usr/local/hadoop/file/usr"));
/*			FileInputFormat.addInputPath(job, new Path("/usr/local/hadoop/file/usr/2017-05-01.txt"));
			FileInputFormat.addInputPath(job, new Path("/usr/local/hadoop/file/usr/2017-05-02.txt"));
			FileInputFormat.addInputPath(job, new Path("/usr/local/hadoop/file/usr/2017-05-03.txt"));*/
			
			FileSystem fs2 = FileSystem.get(conf);
			Path op2 = new Path("/usr/local/hadoop/file/ActiveDevicePlayDay");
			if (fs2.exists(op2)) {
				fs2.delete(op2, true);
				System.out.println("存在此输出路径，已删除！！！");
			}
			FileOutputFormat.setOutputPath(job, op2);
			job.waitForCompletion(true);
			
			/*向数据库写数据操作*/
			Database database = Database.getInstance();
			FileReader file = new FileReader("/usr/local/hadoop/file/ActiveDevicePlayDay/part-r-00000");
			BufferedReader bReader = new BufferedReader(file);
			
			String str = null;
			boolean flag = false;
			while ((str = bReader.readLine()) != null) {
				String[] val = str.split("\\s+");
				String app_key = val[0];
				String one = val[2];
				String twoToThree = bReader.readLine().split("\\s+")[2];
				String fourToSeven = bReader.readLine().split("\\s+")[2];
				String eightToFourteen = bReader.readLine().split("\\s+")[2];
				String fifteenToThirty = bReader.readLine().split("\\s+")[2];
				String ThirtyOneToNinty = bReader.readLine().split("\\s+")[2];
				flag = database.updateActiveDevice(app_key, one, twoToThree, fourToSeven, eightToFourteen, fifteenToThirty, ThirtyOneToNinty);
			}
			System.out.println(flag);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
