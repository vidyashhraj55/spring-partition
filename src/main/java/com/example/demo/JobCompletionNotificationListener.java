package com.example.demo;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

	private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
			log.info("!!! JOB FINISHED! Time to verify the results");


		}
//		 try {
//			checkDatabase();
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
	
	/*private void checkDatabase() throws SQLException {
		Connection connection = jdbcTemplate.getDataSource().getConnection();
		DatabaseMetaData metaData = connection.getMetaData();
		ResultSet tables = metaData.getTables(null, null, null, null);
		Statement st=connection.createStatement();
		while(tables.next())
		{
			String schema=tables.getString("TABLE_SCHEM");
			String name=tables.getString("TABLE_NAME");
			if(!name.startsWith("BATCH_"))
			{
				continue;
			}
			String sql="select * from "+schema+" . "+name;
			System.out.println("***********"+sql);
			ResultSet rs = st.executeQuery(sql);
			ResultSetMetaData rsmd = rs.getMetaData();
			while(rs.next())
			{
				for (int i = 0; i < rsmd.getColumnCount(); i++) 
				{
					System.out.print(rsmd.getColumnName(i+1)+"="+rs.getString(i+1)+", ");
				}
				System.out.println();
			}
			rs.close();
			
		}
		st.close();
		tables.close();
	}*/
}
