/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.simplefs;

import io.datakernel.FileManager;
import io.datakernel.FsServer;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;

public final class SimpleFsServer extends FsServer<SimpleFsServer> {
	public SimpleFsServer(Eventloop eventloop, ExecutorService executor, Path storagePath) {
		super(eventloop, new FileManager(eventloop, executor, storagePath));
	}

	@Override
	public void upload(String fileName, final StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		fileManager.save(fileName, new ForwardingResultCallback<StreamFileWriter>(callback) {
			@Override
			public void onResult(StreamFileWriter writer) {
				writer.setFlushCallback(callback);
				producer.streamTo(writer);
			}
		});
	}

	@Override
	public void download(String fileName, long startPosition, final ResultCallback<StreamProducer<ByteBuf>> callback) {
		fileManager.get(fileName, startPosition, new ForwardingResultCallback<StreamFileReader>(callback) {
			@Override
			public void onResult(StreamFileReader reader) {
				callback.onResult(reader);
			}
		});
	}

	@Override
	public void delete(String fileName, CompletionCallback callback) {
		fileManager.delete(fileName, callback);
	}

	@Override
	protected void list(ResultCallback<List<String>> callback) {
		fileManager.scan(callback);
	}
}
