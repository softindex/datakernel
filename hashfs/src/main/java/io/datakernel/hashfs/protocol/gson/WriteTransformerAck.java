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

package io.datakernel.hashfs.protocol.gson;

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class WriteTransformerAck<I> extends AbstractStreamTransformer_1_1<I, I> {
	private static final Logger logger = LoggerFactory.getLogger(WriteTransformerAck.class);

	private final List<CompletionCallback> callback = new ArrayList<>();
	private boolean parentComplete = false;
	private final CompletionCallback parentFinished = new CompletionCallback() {
		@Override
		public void onComplete() {
			parentComplete = true;
		}

		@Override
		public void onException(Exception exception) {
			for (CompletionCallback requestedCallback : callback) {
				requestedCallback.onException(exception);
			}
		}
	};

	public WriteTransformerAck(Eventloop eventloop) {
		super(eventloop);
		super.addCompletionCallback(parentFinished);
	}

	public void receiveAck() {
		logger.info("Client receive ack for file upload");
		if (parentComplete) {
			for (CompletionCallback requestedCallback : callback) {
				requestedCallback.onComplete();
			}
		} else {
			parentFinished.onException(new Exception("Receive ack from server before complete write"));
		}
	}

	@Override
	public StreamDataReceiver<I> getDataReceiver() {
		return downstreamDataReceiver;
	}

	@Override
	public void onEndOfStream() {
		sendEndOfStream();
	}

	@Override
	public void addCompletionCallback(CompletionCallback completionCallback) {
		callback.add(completionCallback);
	}
}
