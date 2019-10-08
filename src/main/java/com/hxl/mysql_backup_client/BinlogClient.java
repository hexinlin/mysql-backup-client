package com.hxl.mysql_backup_client;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.hxl.mysql_backup_client.util.C3p0Util;
import com.hxl.mysql_backup_client.util.KafkaConsumption;
import com.hxl.mysql_backup_client.util.SqlEntry;
import com.mysql.jdbc.PreparedStatement;

public class BinlogClient {

	private final static Logger _logger = Logger.getLogger(BinaryLogClient.class);
	private static CountDownLatch _latch = null;
	private static KafkaConsumption consumer = new KafkaConsumption();
	private static Map<String,Object> _initParams = new HashMap<String, Object>();
	private static String _init_gtid = null;
	private final static String _insert = "insert into";
	private final static String _update = "update";
	private final static String _delete = "delete from";
	public static void main(String[] args) throws Exception{
		
		
		InputStream inputStream = BinlogClient.class.getClassLoader().getResourceAsStream("app.properties");
		Properties prop = new Properties();
		prop.load(inputStream);
		Iterator<Object> itr = prop.keySet().iterator();
		Object key = null;
		while(itr.hasNext()) {
			key = itr.next();
			_initParams.put((String)key, prop.get(key));
		}
		
		_init_gtid =(String) _initParams.get("gtid");
		
		 /*TopicPartition p = new TopicPartition(consumer.TOPIC,0);
		 consumer.consumer.assign(Arrays.asList(p));
		 consumer.consumer.seek(p, 0);
		*/
		consumer.consumer.subscribe(Collections.singleton(consumer.TOPIC));
		List<SqlEntry> entries = new ArrayList<SqlEntry>();
		String value = null;
		_logger.info("客户端启动成功");
		int index = -1;
		SqlEntry entry = null;
		while (true) {
		     ConsumerRecords<String, String> records = consumer.consumer.poll(1000);
		     if(!records.isEmpty()) {
		    	 entries.clear();
			     for (ConsumerRecord<String, String> record : records) {
			         _logger.info("消费到数据："+record.value());
			         value = record.value();
			         index = value.indexOf("|");
			         entry = new SqlEntry(value.substring(0, index), value.substring(index+1));
			         if(null!=_init_gtid) {
			        	
			        	 if(Integer.parseInt(entry.getGtid().split(":")[1])>=Integer.parseInt(_init_gtid.split(":")[1])) {
			        		 entries.add(entry);
			        	 }
			         }else {
			        	 entries.add(entry);
			         }
			     }
			     if(entries.size()>0) {
			    	 commit();
			    	 work(entries);
			     }else {
			    	 commit();
			     }
			    
			    
		     }
		    
		 }
		
		
		
		
	}
	
	public static void work(List<SqlEntry> entries) {
		 boolean isSuccess = execute(entries);
	     if(isSuccess) {
	    	commit();
	     }else {
	    	 _logger.error("事务执行失败，不能继续拉取消息");
	    	 try {
				TimeUnit.SECONDS.sleep(2);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	    	 _logger.info("再次尝试执行事务");
	    	 work(entries);
	     }
	    
	}
	
	public static void commit() {
		try {
			consumer.consumer.commitSync();
		} catch (Exception e) {
			_logger.error(e);
			commit();
		}
	}
	
	/**
	 * 
	* @Title: execute 
	* @Description: 
	* @param @param entries
	* @param @return 
	* @return boolean 
	* @throws
	 */
	public static boolean execute(List<SqlEntry> entries) {
		_latch = new CountDownLatch(entries.size());
		boolean flag = true;
		Connection connection = null;
		Statement statement = null;
		int i=0;
		 String sql0="SET FOREIGN_KEY_CHECKS=0;";
		 String sql1=null;
		 String sql2=null;
		 String sql3=null;
		 String[] sqls = null;
		try {
			 connection = C3p0Util.getConnection();
			 connection.setAutoCommit(false);
			 SqlEntry entry = null;
			
			 for(;i<entries.size();i++) {
				entry = entries.get(i);
			    statement = connection.createStatement();
			    sql1 = "SET GTID_NEXT= '"+entry.getGtid()+"';";
			    sql2 = entry.getSql().replace("BEGIN;", "").replace("COMMIT;", "");
			   
			    statement.addBatch(sql0);
				statement.addBatch(sql1);
				
				
				
				if(sql2.startsWith("USE")) {
					sql3 = sql2.substring(0, sql2.indexOf(";")+1);
					statement.addBatch(sql3);
					sql2 = sql2.replace(sql3, "");
				}
				//判断一个事务中是否存在多个insert into，update，delete
				if(sql2.trim().startsWith(_insert)) {
					sqls = sql2.split(_insert);
					for(String s:sqls) {
						if(s.trim().length()>0) {
							statement.addBatch(_insert+s);
						}
						
					}
				}else if(sql2.trim().startsWith(_update)) {
					sqls = sql2.split(_update);
					for(String s:sqls) {
						if(s.trim().length()>0) {
							statement.addBatch(_update+s);
						}
						
					}
				}else if(sql2.trim().startsWith(_delete)) {
					sqls = sql2.split(_delete);
					for(String s:sqls) {
						if(s.trim().length()>0) {
							statement.addBatch(_delete+s);
						}
						
					}
				}else {
					statement.addBatch(sql2);
				}
				
				statement.executeBatch();
				connection.commit();
				_latch.countDown();
			}
			_logger.info("--执行成功--");
		} catch (SQLException e) {
			_logger.error("执行失败:"+sql1+"|"+sql3+"|"+sql2+"，"+e);
			flag = false;
		} finally {
			C3p0Util.releaseResource(connection, statement, null);
		}
		return flag;
		
		
	}
	
	
	
}
