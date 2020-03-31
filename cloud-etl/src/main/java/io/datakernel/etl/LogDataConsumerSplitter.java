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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static io.datakernel.common.Preconditions.checkState;

@SuppressWarnings("unchecked")
public abstract class LogDataConsumerSplitter<T, D> implements LogDataConsumer<T, D> {

	public final class Context {
		private final List<LogDataConsumer<?, D>> logDataConsumers = new ArrayList<>();
		private Iterator<? extends StreamDataAcceptor<?>> acceptors;

		@Nullable
		public final <X> StreamDataAcceptor<X> addOutput(LogDataConsumer<X, D> logDataConsumer) {
			if (acceptors == null) {
				// initial run, recording scheme
				logDataConsumers.add(logDataConsumer);
				return null;
			}
			// receivers must correspond outputs for recorded scheme
			return (StreamDataAcceptor<X>) acceptors.next();
		}
	}

	@Override
	public StreamConsumerWithResult<T, List<D>> consume() {
		AsyncCollector<List<D>> diffsCollector = AsyncCollector.create(new ArrayList<>());

		Context ctx = new Context();
		createSplitter(ctx);

		StreamSplitter<T, Object> splitter = StreamSplitter.create(acceptors -> {
			ctx.acceptors = Arrays.asList(acceptors).iterator();
			return createSplitter(ctx);
		});

		checkState(!ctx.logDataConsumers.isEmpty());

		for (LogDataConsumer<?, D> logDataConsumer : ctx.logDataConsumers) {
			diffsCollector.addPromise(
					splitter.newOutput().streamTo(((LogDataConsumer<Object, D>) logDataConsumer).consume()),
					List::addAll);
		}

		return StreamConsumerWithResult.of(splitter.getInput(), diffsCollector.run().get());
	}

	protected abstract StreamDataAcceptor<T> createSplitter(@NotNull Context ctx);

}
