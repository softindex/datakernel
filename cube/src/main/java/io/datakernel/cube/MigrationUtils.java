package io.datakernel.cube;

import io.datakernel.logfs.LogFile;
import io.datakernel.logfs.LogPosition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MigrationUtils {
	public static void load(Connection conn) {
		try (PreparedStatement ps = conn.prepareStatement("SELECT log, partition, file, file_index, position FROM aggregation_db_log")) {
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String log = rs.getString("log");
				String partition = rs.getString("partition");
				String file = rs.getString("file");
				int file_index = rs.getInt("file_index");
				long position = rs.getLong("position");
				LogFile logFile = new LogFile(file, file_index);
				LogPosition logPosition = LogPosition.create(logFile, position);
				
			}
		} catch (SQLException e) {

		}
	}
}
