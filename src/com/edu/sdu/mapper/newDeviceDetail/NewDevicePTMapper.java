package com.edu.sdu.mapper.newDeviceDetail;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.edu.sdu.bean.PlayerDeviceDetailBean;

/**
 * 新设备设备型号统计的mapper
 * @author 王宁
 *
 */
public class NewDevicePTMapper extends Mapper<LongWritable, Text, PlayerDeviceDetailBean, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String val[] = line.split("\\s+");
		
		String deviceType = val[12]; // 设备型号
		String app_key = val[0];
		String state = val[5];
		String time = val[6];
		
		if(state.equals("5"))
			context.write(new PlayerDeviceDetailBean(app_key, deviceType), new Text(time));
	}

}
