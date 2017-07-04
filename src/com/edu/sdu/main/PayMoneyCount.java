package com.edu.sdu.main;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.edu.sdu.mapper.PayUserMapper;
import com.edu.sdu.reducer.PayMoneyReducer;
import com.edu.sdu.util.Database;

public class PayMoneyCount {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "PayMoneyCount");
			job.setJarByClass(PayMoneyCount.class);
			job.setMapperClass(PayUserMapper.class);
			job.setReducerClass(PayMoneyReducer.class);
			
			job.setMapOutputKeyClass(Text.class);// map阶段的输出的key
			job.setMapOutputValueClass(FloatWritable.class);// map阶段的输出的value
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("/usr/local/hadoop/file/usr/1.txt"));
			
			FileSystem fs2 = FileSystem.get(conf);
			Path op2 = new Path("/usr/local/hadoop/file/PayMoneyCount");
			if (fs2.exists(op2)) {
				fs2.delete(op2, true);
				System.out.println("存在此输出路径，已删除！！！");
			}
			FileOutputFormat.setOutputPath(job, op2);
			job.waitForCompletion(true);
			
			/*向数据库写数据操作*/
			Database database = Database.getInstance();
			FileReader file = new FileReader("/usr/local/hadoop/file/PayMoneyCount/part-r-00000");
			BufferedReader bReader = new BufferedReader(file);
			
			String str = null;
			while ((str = bReader.readLine()) != null) {
				String[] val = str.split("\\s+");
				boolean flag = database.updateAppCriticalData(val[0],
						"", "", "", "", "", val[1], "2000-01-01");
				System.out.println(flag);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
