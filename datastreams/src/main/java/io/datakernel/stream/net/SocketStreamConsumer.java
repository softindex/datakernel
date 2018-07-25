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

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamStatus;

final class SocketStreamConsumer extends AbstractStreamConsumer<ByteBuf> implements StreamDataReceiver<ByteBuf> {
	private final AsyncTcpSocket asyncTcpSocket;
	private final SettableStage<Void> sentStage;

	private int writeLoop;

	private boolean sent;

	// region creators
	private SocketStreamConsumer(AsyncTcpSocket asyncTcpSocket,
								 SettableStage<Void> sentStage) {
		this.asyncTcpSocket = asyncTcpSocket;
		this.sentStage = sentStage;
	}

	public static SocketStreamConsumer create(AsyncTcpSocket asyncTcpSocket) {
		return new SocketStreamConsumer(asyncTcpSocket, new SettableStage<>());
	}
	// endregion

	@Override
	protected void onStarted() {
		getProducer().produce(this);
	}

	@Override
	public void onEndOfStream() {
		asyncTcpSocket.writeEndOfStream();
	}

	@Override
	protected void onError(Throwable t) {
		sentStage.setException(t);
	}

	@Override
	public void onData(ByteBuf buf) {
		if (getStatus().isClosed()) {
			buf.recycle();
			return;
		}
		asyncTcpSocket.write(buf);
		int loop = eventloop.getLoop();

		if (writeLoop == 0) {
			writeLoop = loop;
			return;
		}

		if (loop != writeLoop) {
			writeLoop = loop;
			getProducer().suspend();
		}
	}

	public void onWrite() {
		writeLoop = 0;
		if (getStatus().isOpen()) {
			getProducer().produce(this);
		} else if (getStatus() == StreamStatus.END_OF_STREAM) {
			sent = true;
			sentStage.set(null);
		}
	}

	public boolean isClosed() {
		return !isWired() || sent || getStatus() == StreamStatus.CLOSED_WITH_ERROR;
	}

	public Stage<Void> getSentStage() {
		return sentStage;
	}
}
