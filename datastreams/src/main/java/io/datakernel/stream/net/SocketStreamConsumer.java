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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamStatus;

import java.util.concurrent.CompletionStage;

final class SocketStreamConsumer extends AbstractStreamConsumer<ByteBuf> implements StreamDataReceiver<ByteBuf> {
	private final AsyncTcpSocket asyncTcpSocket;
	private final SettableStage<Void> sentStage;

	private int writeTick;

	private boolean sent;

	// region creators
	private SocketStreamConsumer(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket,
	                             SettableStage<Void> sentStage) {
		super(eventloop);
		this.asyncTcpSocket = asyncTcpSocket;
		this.sentStage = sentStage;
	}

	public static SocketStreamConsumer create(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket) {
		return new SocketStreamConsumer(eventloop, asyncTcpSocket, SettableStage.create());
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
	protected void onError(Exception e) {
		sentStage.setException(e);
	}

	@Override
	public void onData(ByteBuf buf) {
		asyncTcpSocket.write(buf);
		int tick = eventloop.getTick();

		if (writeTick == 0) {
			writeTick = tick;
			return;
		}

		if (tick != writeTick) {
			writeTick = tick;
			getProducer().suspend();
		}
	}

	public void onWrite() {
		writeTick = 0;
		if (getStatus() == StreamStatus.SUSPENDED) {
			getProducer().produce(this);
		} else if (getStatus() == StreamStatus.END_OF_STREAM) {
			sent = true;
			sentStage.set(null);
		}
	}

	public boolean isClosed() {
		return !isWired() || sent || getStatus() == StreamStatus.CLOSED_WITH_ERROR;
	}

	public CompletionStage<Void> getSentStage() {
		return sentStage;
	}
}