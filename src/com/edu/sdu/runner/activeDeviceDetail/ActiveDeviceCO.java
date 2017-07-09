package com.edu.sdu.runner.activeDeviceDetail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.edu.sdu.mapper.activeDeviceDetail.ActiveDeviceCOMapper;
import com.edu.sdu.reducer.DetailReducer;
import com.edu.sdu.util.Net;
import com.edu.sdu.util.WriteJson;
import com.sdu.edu.bean.PlayerDeviceDetailBean;
import com.sdu.edu.bean.Sysmbol;

public class ActiveDeviceCO {

	public static void main(String[] args) {
		Sysmbol.startDay = "2017-05-01.txt";
		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "ActiveDeviceCO");
			job.setJarByClass(ActiveDeviceCO.class);
			job.setMapperClass(ActiveDeviceCOMapper.class);
			job.setReducerClass(DetailReducer.class);
			
			job.setMapOutputKeyClass(PlayerDeviceDetailBean.class);// map阶段的输出的key
			job.setMapOutputValueClass(Text.class);// map阶段的输出的value
			job.setOutputKeyClass(PlayerDeviceDetailBean.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("/usr/local/hadoop/file/usr/" + Sysmbol.startDay));
			
			FileSystem fs2 = FileSystem.get(conf);
			Path op2 = new Path("/usr/local/hadoop/file/ActiveDeviceCO");
			if (fs2.exists(op2)) {
				fs2.delete(op2, true);
				System.out.println("存在此输出路径，已删除！！！");
			}
			FileOutputFormat.setOutputPath(job, op2);
			job.waitForCompletion(true);
			
			String fileName = Sysmbol.startDay.substring(0, Sysmbol.startDay.length() - 4) + 
					"_ad_co.txt";
			String filePath = "/usr/local/hadoop/file/ActiveDeviceCO/" + fileName;
			WriteJson.doWriteFile("/usr/local/hadoop/file/ActiveDeviceCO/part-r-00000", filePath);
			
			Net.upLoadJsonFile(filePath, fileName, "ad");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
