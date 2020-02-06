/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.etl;

import io.datakernel.async.process.AsyncCollector;
import io.datakernel.datastream.StreamConsumerWithResult;
import io.datakernel.datastream.StreamDataAcceptor;
import io.datakernel.datastream.processor.StreamSplitter;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.datakernel.common.Preconditions.checkState;
import static java.util.Arrays.asList;

@SuppressWarnings("unchecked")
public abstract class LogDataConsumerSplitter<T, D> implements LogDataConsumer<T, D> {
	private final List<LogDataConsumer<?, D>> logDataConsumers = new ArrayList<>();

	protected static final class Context {
		private final Iterator<? extends StreamDataAcceptor<?>> receivers;

		public Context(List<StreamDataAcceptor<?>> receivers) {
			this.receivers = receivers.iterator();
		}
	}

	@Override
	public StreamConsumerWithResult<T, List<D>> consume() {
		if (logDataConsumers.isEmpty()) {
			createSplitter(null); // recording scheme
			checkState(!logDataConsumers.isEmpty(), "addOutput() should be called at least once");
		}

		AsyncCollector<List<D>> diffsCollector = AsyncCollector.create(new ArrayList<>());

		StreamSplitter<T, Object> splitter = StreamSplitter.create(acceptors ->
				createSplitter(new Context(asList(acceptors))));

		for (LogDataConsumer<?, D> logDataConsumer : logDataConsumers) {
			diffsCollector.addPromise(
					splitter.newOutput().streamTo(((LogDataConsumer<Object, D>) logDataConsumer).consume()),
					List::addAll);
		}

		return StreamConsumerWithResult.of(splitter.getInput(), diffsCollector.run().get());
	}

	protected abstract StreamDataAcceptor<T> createSplitter(Context ctx);

	@Nullable
	protected final <X> StreamDataAcceptor<X> addOutput(Context ctx, LogDataConsumer<X, D> logDataConsumer) {
		if (ctx == null) {
			// initial run, recording scheme
			logDataConsumers.add(logDataConsumer);
			return null;
		}
		// receivers must correspond outputs for recorded scheme
		return (StreamDataAcceptor<X>) ctx.receivers.next();
	}

}
