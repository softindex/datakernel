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

import io.datakernel.eventloop.Eventloop;

public final class SimpleStreamConsumer<T> extends AbstractStreamConsumer<T> {

	public interface StatusListener {
		void onEndOfStream();

		void onClosedWithError(Exception e);
	}

	public interface StatusListenerWithDataReceiver<T> extends StreamDataReceiver<T>, StatusListener {
	}

	private final StatusListener statusListener;
	private final StreamDataReceiver<T> streamDataReceiver;

	// region creators
	private SimpleStreamConsumer(Eventloop eventloop, StatusListener statusListener,
	                             StreamDataReceiver<T> streamDataReceiver) {
		super(eventloop);
		this.statusListener = statusListener;
		this.streamDataReceiver = streamDataReceiver;
	}

	public static <T> SimpleStreamConsumer<T> create(Eventloop eventloop, StatusListener statusListener,
	                                                 StreamDataReceiver<T> streamDataReceiver) {
		return new SimpleStreamConsumer<T>(eventloop, statusListener, streamDataReceiver);
	}
	// endregion

	@Override
	protected void onEndOfStream() {
		if (statusListener != null)
			statusListener.onEndOfStream();
	}

	@Override
	protected void onError(Exception e) {
		if (statusListener != null)
			statusListener.onClosedWithError(e);
	}

	@Override
	public StreamDataReceiver<T> getDataReceiver() {
		return streamDataReceiver;
	}

	@Override
	public void suspend() {
		super.suspend();
	}

	@Override
	public void resume() {
		super.resume();
	}
}
