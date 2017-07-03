package com.edu.sdu.main;

import java.io.IOException;

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

import com.edu.sdu.mapper.DedupMapper;
import com.edu.sdu.mapper.NewUserMapper;
import com.edu.sdu.reducer.DedupReducer;
import com.edu.sdu.reducer.NewUserReducer;

public class NewUser {

	public static void main(String[] args) {
		JobConf conf = new JobConf(NewUser.class);
		conf.set("mapred.job.tracker", "127.0.0.1:9001");
		try {
			String[] dedupArgs = new String[] { "/usr/local/hadoop/file/today.txt", "/usr/local/hadoop/file/dedup" };
			Job dedupJob = new Job(conf, "DataDeduplication");
			dedupJob.setJarByClass(NewUser.class);
			// 设置Map、Combine和Reduce处理类
			dedupJob.setMapperClass(DedupMapper.class);
			dedupJob.setReducerClass(DedupReducer.class);

			dedupJob.setMapOutputKeyClass(Text.class);// map阶段的输出的key
			dedupJob.setMapOutputValueClass(Text.class);// map阶段的输出的value
			// 设置输出类型
			dedupJob.setOutputKeyClass(Text.class);
			dedupJob.setOutputValueClass(Text.class);

			ControlledJob ctrljob1 = new ControlledJob(conf);
			ctrljob1.setJob(dedupJob);

			// 设置输入和输出目录
			FileInputFormat.addInputPath(dedupJob, new Path(dedupArgs[0]));
			FileSystem fs = FileSystem.get(conf);
			Path op = new Path(dedupArgs[1]);
			if (fs.exists(op)) {
				fs.delete(op, true);
				System.out.println("存在此输出路径，已删除！！！");
			}
			FileOutputFormat.setOutputPath(dedupJob, op);

			/** ============================================================ **/

			String[] newUserArgs = new String[] { "/usr/local/hadoop/file/pre.txt",
					"/usr/local/hadoop/file/dedup/part-r-00000", "/usr/local/hadoop/file/newUser" };
			Job newUserJob = new Job(conf, "NewUser");
			newUserJob.setJarByClass(NewUser.class);
			// 设置Map、Combine和Reduce处理类
			newUserJob.setMapperClass(NewUserMapper.class);
			newUserJob.setReducerClass(NewUserReducer.class);

			newUserJob.setMapOutputKeyClass(Text.class);// map阶段的输出的key
			newUserJob.setMapOutputValueClass(Text.class);// map阶段的输出的value

			// 设置输出类型
			newUserJob.setOutputKeyClass(Text.class);
			newUserJob.setOutputValueClass(Text.class);

			ControlledJob ctrljob2 = new ControlledJob(conf);
			ctrljob2.setJob(newUserJob);
			ctrljob2.addDependingJob(ctrljob1); // job2在job1完成后，才可以启动

			FileSystem fs2 = FileSystem.get(conf);

			Path op2 = new Path(newUserArgs[2]);
			if (fs2.exists(op2)) {
				fs2.delete(op2, true);
				System.out.println("存在此输出路径，已删除！！！");
			}

			// 设置输入和输出目录
			FileInputFormat.addInputPath(newUserJob, new Path(newUserArgs[0]));
			FileInputFormat.addInputPath(newUserJob, new Path(newUserArgs[1]));
			FileOutputFormat.setOutputPath(newUserJob, op2);

			JobControl jobCtrl = new JobControl("myctrl");
			
			// 添加到总的JobControl里，进行控制
			jobCtrl.addJob(ctrljob1);
			jobCtrl.addJob(ctrljob2);
			// 在线程启动
			Thread t = new Thread(jobCtrl);
			t.start();
			while (true) {
				if (jobCtrl.allFinished()) {
					System.out.println(jobCtrl.getSuccessfulJobList());
					jobCtrl.stop();
					return;
				}
				if (jobCtrl.getFailedJobList().size() > 0) {
					System.out.println(jobCtrl.getFailedJobList());
					jobCtrl.stop();
					return;
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
