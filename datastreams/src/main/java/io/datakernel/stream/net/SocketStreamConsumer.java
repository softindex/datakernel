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
	private final SettableStage<Void> completionStage;

	private int writeTick;

	private boolean closedProperly;

	// region creators
	private SocketStreamConsumer(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket,
	                             SettableStage<Void> completionStage) {
		super(eventloop);
		this.asyncTcpSocket = asyncTcpSocket;
		this.completionStage = completionStage;
	}

	public static SocketStreamConsumer create(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket) {
		return new SocketStreamConsumer(eventloop, asyncTcpSocket, SettableStage.create());
	}
	// endregion

	@Override
	public StreamDataReceiver<ByteBuf> getDataReceiver() {
		return this;
	}

	@Override
	public void onEndOfStream() {
		asyncTcpSocket.writeEndOfStream();
	}

	@Override
	protected void onError(Exception e) {
		completionStage.setError(e);
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
			suspend();
		}
	}

	public void onWrite() {
		writeTick = 0;
		if (getConsumerStatus() == StreamStatus.SUSPENDED) {
			resume();
		} else if (getConsumerStatus() == StreamStatus.END_OF_STREAM) {
			closedProperly = true;
			completionStage.setResult(null);
		}
	}

	public boolean isClosed() {
		return closedProperly || getConsumerStatus() == StreamStatus.CLOSED_WITH_ERROR;
	}

	public CompletionStage<Void> getCompletionStage() {
		return completionStage;
	}
}