package com.hxl.mysql_backup_client.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class C3p0Util {

	private static ComboPooledDataSource dataSource = null;
	private final static Logger _logger = Logger.getLogger(C3p0Util.class);
	static {
		dataSource = new ComboPooledDataSource("mysql");
	}

	/**
	 * 
	 * @return 返回一个连接池就是上面创建的连接池
	 */
	public static ComboPooledDataSource getDataSource() {
		// 返回这个连接池
		return dataSource;
	}

	/**
	 * 
	 * @return 返回一个从数据库里拿出来的连接
	 * @throws SQLException
	 */
	public static Connection getConnection() throws SQLException {
		// 返回一条连接
		return dataSource.getConnection();
	}

	public static void releaseResource(Connection conn, Statement stmt,
			ResultSet rs) {

		try {
			if (rs != null)
				rs.close();
			if (stmt != null)
				stmt.close();
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			_logger.error(e);
		}

	}
}
