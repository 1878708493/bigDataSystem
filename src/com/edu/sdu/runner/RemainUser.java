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

import com.edu.sdu.mapper.RemainDeviceMapper;
import com.edu.sdu.mapper.RemainUserMapper;
import com.edu.sdu.reducer.RemainDeviceReducer;
import com.edu.sdu.reducer.RemainUserReducer;
import com.edu.sdu.util.Database;
import com.edu.sdu.util.Tool;
import com.sdu.edu.bean.RemainOprBean;
import com.sdu.edu.bean.Sysmbol;

public class RemainUser {

	public static void main(String[] args) {
		try {
			Sysmbol.startDay = "2017-05-01.txt";
			Sysmbol.endDay = "2017-05-04.txt";
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "RemainUser");
			job.setJarByClass(RemainUser.class);
			job.setMapperClass(RemainUserMapper.class);
			job.setReducerClass(RemainUserReducer.class);
			
			job.setMapOutputKeyClass(Text.class);// map阶段的输出的key
			job.setMapOutputValueClass(RemainOprBean.class);// map阶段的输出的value
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("/usr/local/hadoop/file/usr/" + Sysmbol.startDay));
			FileInputFormat.addInputPath(job, new Path("/usr/local/hadoop/file/usr/" + Sysmbol.endDay));
			
			FileSystem fs2 = FileSystem.get(conf);
			Path op2 = new Path("/usr/local/hadoop/file/RemainUser");
			if (fs2.exists(op2)) {
				fs2.delete(op2, true);
				System.out.println("存在此输出路径，已删除！！！");
			}
			FileOutputFormat.setOutputPath(job, op2);
			job.waitForCompletion(true);
			
			/*向数据库写数据操作*/
			Database database = Database.getInstance();
			FileReader file = new FileReader("/usr/local/hadoop/file/RemainUser/part-r-00000");
			BufferedReader bReader = new BufferedReader(file);
			
			String str = null;
			String start = Sysmbol.startDay.substring(0, Sysmbol.startDay.length() - 4);
			String end = Sysmbol.endDay.substring(0, Sysmbol.startDay.length() - 4);
			System.out.println(start);
			System.out.println(end);
			String date = start;
			int space = Tool.getSpaceDay(start, end);
			boolean flag = false;
			while ((str = bReader.readLine()) != null) {
				String[] val = str.split("\\s+");
				String app_key = val[0];
				String count = val[1];
				
				String oneday = "", twoday = "", threeday = "", fourday = "",
						fiveday = "", sixday = "", sevenday = "";
				
				if(space == 1)
					oneday = count;
				else if (space == 2)
					twoday = count;
				else if (space == 3)
					threeday = count;
				else if (space == 4)
					fourday = count;
				else if (space == 5)
					fiveday = count;
				else if (space == 6)
					sixday = count;
				else if (space == 7)
					sevenday = count;
				flag = database.updateRemainUser(app_key, date, "", oneday, twoday, 
						threeday, fourday, fiveday, sixday, sevenday);
			}
			System.out.println(flag);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
