package io.datakernel.logfs;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractRemoteLogFileSystem extends AbstractLogFileSystem {
	private static final String LOG_NAME_DELIMITER = "/";

	private final String logName;

	protected AbstractRemoteLogFileSystem(String logName) {
		this.logName = logName;
	}

	protected String path(String logPartition, LogFile logFile) {
		return logName + LOG_NAME_DELIMITER + fileName(logPartition, logFile);
	}

	protected List<LogFile> getLogFiles(List<String> fileNames, String logPartition) {
		List<LogFile> entries = new ArrayList<>();
		for (String file : fileNames) {
			String[] splittedFileName = file.split(LOG_NAME_DELIMITER);
			String fileLogName = splittedFileName[0];

			if (!fileLogName.equals(logName))
				continue;

			PartitionAndFile partitionAndFile = parse(splittedFileName[1]);
			if (partitionAndFile != null && partitionAndFile.logPartition.equals(logPartition)) {
				entries.add(partitionAndFile.logFile);
			}
		}
		return entries;
	}
}
