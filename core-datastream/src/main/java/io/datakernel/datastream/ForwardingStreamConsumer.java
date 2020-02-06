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

package io.datakernel.datastream;

import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;

public abstract class ForwardingStreamConsumer<T> implements StreamConsumer<T> {
	protected final StreamConsumer<T> consumer;

	public ForwardingStreamConsumer(StreamConsumer<T> consumer) {
		this.consumer = consumer;
	}

	@Override
	public void consume(@NotNull StreamDataSource<T> dataSource) {
		consumer.consume(dataSource);
	}

	@Override
	public void endOfStream() {
		consumer.endOfStream();
	}

	@Override
	public Promise<Void> getAcknowledgement() {
		return consumer.getAcknowledgement();
	}

	@Override
	public void close(@NotNull Throwable e) {
		consumer.close(e);
	}
}
