package io.datakernel.logfs;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.hashfs.HashFsClient;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

import java.util.List;

public final class HashFsLogFileSystem extends AbstractRemoteLogFileSystem {
	private final HashFsClient client;

	public HashFsLogFileSystem(HashFsClient client, String logName) {
		super(logName);
		this.client = client;
	}

	@Override
	public void list(final String logPartition, final ResultCallback<List<LogFile>> callback) {
		client.list(new ResultCallback<List<String>>() {
			@Override
			public void onResult(List<String> files) {
				callback.onResult(getLogFiles(files, logPartition));
			}

			@Override
			public void onException(Exception exception) {
				callback.onException(exception);
			}
		});
	}

	@Override
	public void read(String logPartition, LogFile logFile, long startPosition, StreamConsumer<ByteBuf> consumer, ResultCallback<Long> positionCallback) {
		client.download(path(logPartition, logFile), startPosition, consumer, positionCallback);
	}

	@Override
	public void write(String logPartition, LogFile logFile, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		client.upload(path(logPartition, logFile), producer, callback);
	}
}
