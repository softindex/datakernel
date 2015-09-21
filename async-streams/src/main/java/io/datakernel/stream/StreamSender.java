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

package io.datakernel.stream;

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.NioEventloop;

import java.util.ArrayDeque;

public class StreamSender<T> extends AbstractStreamProducer<T> {
	private final ArrayDeque<QueueEntry> queue = new ArrayDeque<>();
	private boolean sendEndOfStream;

	private class QueueEntry {
		private T item;
		private CompletionCallback callback;

		public QueueEntry(T item, CompletionCallback callback) {
			this.item = item;
			this.callback = callback;
		}
	}

	public StreamSender(NioEventloop eventloop) {
		this(eventloop, false);
	}

	public StreamSender(NioEventloop eventloop, boolean sendEndOfStream) {
		super(eventloop);
		this.sendEndOfStream = sendEndOfStream;
	}

	@Override
	protected void doProduce() {
		while (!queue.isEmpty()) {
			if (status != READY)
				return;

			T item = retrieveFromBuffer();
			if (item != null)
				doSend(item);
		}

		if (sendEndOfStream)
			sendEndOfStream();
	}

	public void forceSendBuffer() {
		while (!queue.isEmpty()) {
			T item = retrieveFromBuffer();
			if (item != null)
				doSend(item);
		}
	}

	public void forceSend(T item) {
		doSend(item);
	}

	public void send(T item, CompletionCallback callback) {
		if (status == READY) {
			doSend(item);
			callback.onComplete();
			return;
		}

		if (status == SUSPENDED) {
			addToBuffer(item, callback);
		} else {
			callback.onException(new IllegalStateException("StreamSender closed."));
		}
	}

	public boolean trySend(T item) {
		if (status == READY) {
			doSend(item);
			return true;
		}

		if (status == SUSPENDED) {
			addToBuffer(item, null);
		} else {
			queue.clear();
		}
		return false;
	}

	public void sendLater(T item) {
		send(item, null);
	}

	public int getNumberOfBufferedItems() {
		return queue.size();
	}

	public void endStreaming() {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				sendEndOfStream();
			}
		});
	}

	private void addToBuffer(T item, CompletionCallback callback) {
		queue.add(new QueueEntry(item, callback));
	}

	private T retrieveFromBuffer() {
		QueueEntry poppedEntry = queue.poll();
		if (poppedEntry != null && poppedEntry.callback != null)
			poppedEntry.callback.onComplete();
		return poppedEntry == null ? null : poppedEntry.item;
	}

	private void doSend(T item) {
		downstreamDataReceiver.onData(item);
	}

	@Override
	protected void onProducerStarted() {
		produce();
	}

	@Override
	protected void onResumed() {
		resumeProduce();
	}
}
