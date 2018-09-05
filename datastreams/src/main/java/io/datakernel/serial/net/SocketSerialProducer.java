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

package io.datakernel.serial.net;

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.serial.SerialSupplier;

final class SocketSerialProducer implements SerialSupplier<ByteBuf> {
	private final AsyncTcpSocket asyncTcpSocket;
	protected final ByteBufQueue readQueue = new ByteBufQueue();

	private SettableStage<ByteBuf> read;

	private final SettableStage<Void> endOfStream = new SettableStage<>();
	private boolean readEndOfStream;

	// region creators
	SocketSerialProducer(AsyncTcpSocket asyncTcpSocket) {
		this.asyncTcpSocket = asyncTcpSocket;
	}

	// endregion

	@Override
	public Stage<ByteBuf> get() {
		if (readQueue.hasRemaining()) {
			return Stage.of(readQueue.take());
		}
		if (readEndOfStream) {
			endOfStream.trySet(null);
			return Stage.of(null);
		}
		asyncTcpSocket.read();
		read = new SettableStage<>();
		read.thenRunEx(() -> read = null);
		return read;
	}

	@Override
	public void closeWithError(Throwable e) {
		readQueue.recycle();
		if (read != null) {
			read.setException(e);
		}
		endOfStream.trySetException(e);
	}

	private boolean isExhausted() {
		return readQueue.isEmpty();
	}

	public void onRead(ByteBuf buf) {
		readQueue.add(buf);
		if (read != null) {
			read.set(readQueue.take());
			if (isExhausted()) {
				asyncTcpSocket.read();
			}
		}
	}

	public void onReadEndOfStream() {
		this.readEndOfStream = true;
		if (read != null && readQueue.isEmpty()) {
			read.set(null);
			endOfStream.trySet(null);
		}
	}

	public Stage<Void> getEndOfStream() {
		return endOfStream;
	}

	public boolean isClosed() {
		return endOfStream.isComplete();
	}

}
