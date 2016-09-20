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

package io.datakernel.stream.net;

import io.datakernel.async.CompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamProducer;

final class SocketStreamProducer extends AbstractStreamProducer<ByteBuf> {
	private final CompletionCallback completionCallback;
	private final AsyncTcpSocket asyncTcpSocket;
	protected final ByteBufQueue readQueue = ByteBufQueue.create();
	private boolean readEndOfStream;

	// region creators
	private SocketStreamProducer(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, CompletionCallback completionCallback) {
		super(eventloop);
		this.asyncTcpSocket = asyncTcpSocket;
		this.completionCallback = completionCallback;
	}

	public static SocketStreamProducer create(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket,
	                                          CompletionCallback completionCallback) {
		return new SocketStreamProducer(eventloop, asyncTcpSocket, completionCallback);
	}
	// endregion

	@Override
	protected void onStarted() {
		produce();
	}

	@Override
	protected void onDataReceiverChanged() {
	}

	@Override
	protected void onSuspended() {
	}

	@Override
	protected void onResumed() {
		resumeProduce();
	}

	@Override
	protected void onError(Exception e) {
		completionCallback.onException(e);
	}

	@Override
	protected void onEndOfStream() {
		completionCallback.onComplete();
	}

	@Override
	public void closeWithError(Exception e) {
		super.closeWithError(e);
	}

	@Override
	protected void doProduce() {
		while (isStatusReady() && readQueue.hasRemaining()) {
			ByteBuf buf = readQueue.take();
			send(buf);
		}
		if (readEndOfStream) {
			if (readQueue.hasRemaining()) {
				ByteBuf buf = readQueue.takeRemaining();
				send(buf);
			}
			sendEndOfStream();
		} else if (readQueue.remainingBufs() <= 1) {
			asyncTcpSocket.read();
		}
	}

	public void onRead(ByteBuf buf) {
		readQueue.add(buf);
		doProduce();
	}

	public void onReadEndOfStream() {
		this.readEndOfStream = true;
		doProduce();
	}

}
