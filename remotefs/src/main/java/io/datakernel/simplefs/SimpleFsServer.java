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
import io.datakernel.eventloop.InetAddressRange;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;

import javax.net.ssl.SSLContext;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

public final class SimpleFsServer extends FsServer<SimpleFsServer> {
	// region builders
	private SimpleFsServer(Eventloop eventloop, ExecutorService executor, Path storagePath) {
		super(eventloop, FileManager.create(eventloop, executor, storagePath));
	}

	private SimpleFsServer(Eventloop eventloop,
	                       ServerSocketSettings serverSocketSettings, SocketSettings socketSettings,
	                       boolean acceptOnce, Collection<InetSocketAddress> listenAddresses,
	                       InetAddressRange range, Collection<InetAddress> bannedAddresses,
	                       SSLContext sslContext, ExecutorService sslExecutor,
	                       Collection<InetSocketAddress> sslListenAddresses,
	                       SimpleFsServer previousInstance) {
		super(eventloop, serverSocketSettings, socketSettings, acceptOnce, listenAddresses,
				range, bannedAddresses, sslContext, sslExecutor, sslListenAddresses, previousInstance);
	}

	public static SimpleFsServer create(Eventloop eventloop, ExecutorService executor, Path storagePath) {
		return new SimpleFsServer(eventloop, executor, storagePath);
	}

	@Override
	protected SimpleFsServer recreate(Eventloop eventloop, ServerSocketSettings serverSocketSettings, SocketSettings socketSettings,
	                                  boolean acceptOnce,
	                                  Collection<InetSocketAddress> listenAddresses,
	                                  InetAddressRange range, Collection<InetAddress> bannedAddresses,
	                                  SSLContext sslContext, ExecutorService sslExecutor,
	                                  Collection<InetSocketAddress> sslListenAddresses) {
		return new SimpleFsServer(eventloop, serverSocketSettings, socketSettings, acceptOnce, listenAddresses,
				range, bannedAddresses, sslContext, sslExecutor, sslListenAddresses, this);
	}
	// endregion

	@Override
	public void upload(String fileName, final ResultCallback<StreamConsumer<ByteBuf>> callback) {
		fileManager.save(fileName, new ForwardingResultCallback<StreamFileWriter>(callback) {
			@Override
			public void onResult(StreamFileWriter result) {
				callback.onResult(result);
			}
		});
	}

	@Override
	public void download(String fileName, long startPosition, final ResultCallback<StreamProducer<ByteBuf>> callback) {
		fileManager.get(fileName, startPosition, new ForwardingResultCallback<StreamFileReader>(callback) {
			@Override
			public void onResult(StreamFileReader result) {
				callback.onResult(result);
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
