package com.edu.sdu.runner.newDeviceDetail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.edu.sdu.bean.PlayerDeviceDetailBean;
import com.edu.sdu.bean.RemainOprBean;
import com.edu.sdu.bean.Sysmbol;
import com.edu.sdu.mapper.RemainDeviceMapper;
import com.edu.sdu.mapper.newDeviceDetail.NewDevicePTMapper;
import com.edu.sdu.reducer.DetailReducer;
import com.edu.sdu.reducer.RemainDeviceReducer;
import com.edu.sdu.util.Net;
import com.edu.sdu.util.WriteJson;

/**
 * 新设备 设备型号统计
 * @author 王宁
 *
 */
public class NewDevicePT {

	public static void main(String[] args) {
		Sysmbol.startDay = args[0];
		Sysmbol.endDay = args[1];
		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "NewDevicePT");
			job.setJarByClass(NewDevicePT.class);
			job.setMapperClass(NewDevicePTMapper.class);
			job.setReducerClass(DetailReducer.class);
			
			job.setMapOutputKeyClass(PlayerDeviceDetailBean.class);// map阶段的输出的key
			job.setMapOutputValueClass(Text.class);// map阶段的输出的value
			job.setOutputKeyClass(PlayerDeviceDetailBean.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(args[2]));
			
			FileSystem fs2 = FileSystem.get(conf);
			Path op2 = new Path(args[3]);
			if (fs2.exists(op2)) {
				fs2.delete(op2, true);
				System.out.println("存在此输出路径，已删除！！！");
			}
			FileOutputFormat.setOutputPath(job, op2);
			job.waitForCompletion(true);

			String fileName = Sysmbol.startDay.substring(0, Sysmbol.startDay.length() - 4) + 
					"_nd_pt.txt";
			String filePath = args[3] + "/" + fileName;
			WriteJson.doWriteFile(args[3] + "/part-r-00000", filePath);
			
			Net.upLoadJsonFile(filePath, fileName, "nd");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
