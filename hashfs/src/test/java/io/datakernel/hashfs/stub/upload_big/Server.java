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

package io.datakernel.hashfs.stub.upload_big;

import io.datakernel.async.CompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileWriter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;

public class Server {
	private final NioEventloop eventloop;
	private final ExecutorService executor;

	public Server(NioEventloop eventloop, ExecutorService executor) {
		this.eventloop = eventloop;
		this.executor = executor;
	}

	public void onUpload(final String destinationfilename, StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		Path destination = Paths.get(destinationfilename);
		StreamConsumer<ByteBuf> diskWrite = StreamFileWriter.createFile(eventloop, executor, destination, true);
		producer.streamTo(diskWrite);
		//diskWrite.addCompletionCallback(callback);
	}

}
