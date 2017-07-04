package com.edu.sdu.util;

import java.sql.*;

import javax.annotation.Nonnull;

import org.apache.hadoop.yarn.server.applicationhistoryservice.webapp.AppPage;

public class Database {
	private static Connection conn;
	private static Database datamain;

	public static Database getInstance() {
		if (datamain == null)
			datamain = new Database();
		return datamain;
	}

	// 连接数据库
	public Database() {
		try {
			String name1 = "root";
			String password1 = "687853";
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://123.206.184.214:3306/summer?useUnicode=true&characterEncoding=utf-8";
			conn = DriverManager.getConnection(url, name1, password1);
			if (conn != null)
				System.out.println("数据库连接成功！");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean updateAppCriticalData(String app_key, String newUser, String newDevice,
			String activeUser, String activeDevice, String payCount, String payMoney, String datetime) {
		int i = 0;
		try {
			String str = "select * from app_critical_data where app_key='" + app_key + "' and datetime='" + datetime + "'";
			Statement state = conn.createStatement();
			ResultSet rs = state.executeQuery(str);
			
			while (rs.next()) {
				i++;
			}
			if(i > 0){	// 有这条数据,只用更新就行
				int length = 0;
				str = "update app_critical_data t set";
				if(!newUser.equals("")) {
					length++;
					str = str.concat(" t.new_user='" + newUser + "'");
				}
				if(!newDevice.equals("")) {
					if(length > 0)
						str = str.concat(",");
					length++;
					str = str.concat(" t.new_device='" + newDevice + "'");
				}
				if(!activeUser.equals("")) {
					if(length > 0)
						str = str.concat(",");
					length++;
					str = str.concat(" t.active_user='" + activeUser + "'");
				}
				if(!activeDevice.equals("")) {
					if(length > 0)
						str = str.concat(",");
					length++;
					str = str.concat(" t.active_device='" + activeDevice + "'");
				}
				if(!payCount.equals("")) {
					if(length > 0)
						str = str.concat(",");
					length++;
					str = str.concat(" t.pay_user_count='" + payCount + "'");
				}
				if(!payMoney.equals("")) {
					if(length > 0)
						str = str.concat(",");
					length++;
					str = str.concat(" t.pay_money='" + payMoney + "'");
				}
				str = str.concat(" where t.app_key='" + app_key + "' and t.datetime='" + datetime + "'");
				System.out.println(str);
				PreparedStatement update = conn.prepareStatement(str); 
				update.execute();
				update.close();
			}
			else {
				str = "insert into app_critical_data(app_key, new_user, new_device, active_user, active_device, pay_user_count, pay_money, datetime)values('" + 
						app_key + "', '" + newUser + "', '" + newDevice + "', '" + activeUser + "', '" + 
						activeDevice + "', '" + payCount + "', '" + payMoney + "', '" + datetime + "')"; 
				state.executeUpdate(str);
				state.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return true;
	}
	
	public boolean updateNewUserData(String app_key, String one_fours, String five_tens, String eleven_thirtys, 
			String thirty_sixtys, String one_threem, String four_tenm, String eleven_thirtym, 
			String thirty_sixtym, String sixtym_more, String datetime) {
		int i = 0;
		try {
			String str = "select * from new_user where app_key='" + app_key + "' and datetime='" + datetime + "'";
			Statement state = conn.createStatement();
			ResultSet rs = state.executeQuery(str);
			
			while (rs.next()) {
				i++;
			}
			if(i > 0){	// 有这条数据,只用更新就行
				int length = 0;
				str = "update new_user set " + "1_4s='" + one_fours + "', " + "5_10s='" + five_tens + "', " + 
						"11_30s='" + eleven_thirtys + "', " + "31_60s='" + thirty_sixtys + "', " + 
						"1_3m='" + one_threem + "', " + "4_10m='" + four_tenm + "', " +
						"11_30m='" + eleven_thirtym + "', " + "31_60m='" + thirty_sixtym + "', " + 
						"60_m='" + sixtym_more + "'";
				str = str.concat(" where app_key='" + app_key + "' and datetime='" + datetime + "'");
				System.out.println(str);
				PreparedStatement update = conn.prepareStatement(str); 
				update.execute();
				update.close();
			}
			else {
				str = "insert into new_user(app_key, 1_4s, 5_10s, 11_30s, 31_60s, 1_3m, 4_10m, 11_30m, 31_60m, 60_m, datetime)values('" + 
						app_key + "', '" + one_fours + "', '" + five_tens + "', '" + eleven_thirtys + "', '" + 
						thirty_sixtys + "', '" + one_threem + "', '" + four_tenm + "', '" + eleven_thirtym + "', '" +
						thirty_sixtym + "', '" + sixtym_more + "', '" + datetime + "')";
				state.executeUpdate(str);
				state.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return true;
	}
}
