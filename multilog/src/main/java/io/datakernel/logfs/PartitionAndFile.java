package io.datakernel.logfs;

final class PartitionAndFile {
	private final String logPartition;
	private final LogFile logFile;

	public PartitionAndFile(String logPartition, LogFile logFile) {
		this.logPartition = logPartition;
		this.logFile = logFile;
	}

	public String getLogPartition() {
		return logPartition;
	}

	public LogFile getLogFile() {
		return logFile;
	}
}
