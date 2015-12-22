package io.datakernel.logfs;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.simplefs.SimpleFsClient;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

import java.util.ArrayList;
import java.util.List;

public final class SimpleFsLogFileSystem extends AbstractLogFileSystem {
	private static final String LOG_NAME_DELIMITER = "/";

	private final SimpleFsClient client;
	private final String logName;

	public SimpleFsLogFileSystem(SimpleFsClient client, String logName) {
		this.client = client;
		this.logName = logName;
	}

	private String path(String logPartition, LogFile logFile) {
		return logName + LOG_NAME_DELIMITER + fileName(logPartition, logFile);
	}

	@Override
	public void list(final String logPartition, final ResultCallback<List<LogFile>> callback) {
		client.list(new ResultCallback<List<String>>() {
			@Override
			public void onResult(List<String> files) {
				List<LogFile> entries = new ArrayList<>();
				for (String file : files) {
					String[] splittedFileName = file.split(LOG_NAME_DELIMITER);
					String fileLogName = splittedFileName[0];

					if (!fileLogName.equals(logName))
						continue;

					PartitionAndFile partitionAndFile = parse(splittedFileName[1]);
					if (partitionAndFile != null && partitionAndFile.logPartition.equals(logPartition)) {
						entries.add(partitionAndFile.logFile);
					}
				}
				callback.onResult(entries);
			}

			@Override
			public void onException(Exception exception) {
				callback.onException(exception);
			}
		});
	}

	@Override
	public void read(String logPartition, LogFile logFile, long startPosition, StreamConsumer<ByteBuf> consumer,
	                 ResultCallback<Long> positionCallback) {
		client.download(path(logPartition, logFile), startPosition, consumer, positionCallback);
	}

	@Override
	public void write(String logPartition, LogFile logFile, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		client.upload(path(logPartition, logFile), producer, callback);
	}
}
