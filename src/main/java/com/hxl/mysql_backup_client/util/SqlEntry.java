package com.hxl.mysql_backup_client.util;

public class SqlEntry {

	private String gtid;
	private String sql;
	
	public SqlEntry(String id,String sql) {
		this.gtid = id;
		this.sql =sql;
	}

	

	public String getGtid() {
		return gtid;
	}



	public void setGtid(String gtid) {
		this.gtid = gtid;
	}



	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}
	
	
}
