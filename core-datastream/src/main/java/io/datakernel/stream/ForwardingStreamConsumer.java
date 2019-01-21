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

import io.datakernel.async.MaterializedPromise;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

public abstract class ForwardingStreamConsumer<T> implements StreamConsumer<T> {
	private StreamConsumer<T> consumer;

	public ForwardingStreamConsumer(StreamConsumer<T> consumer) {
		this.consumer = consumer;
	}

	@Override
	public void setSupplier(StreamSupplier<T> supplier) {
		consumer.setSupplier(supplier);
	}

	@Override
	public MaterializedPromise<Void> getAcknowledgement() {
		return consumer.getAcknowledgement();
	}

	@Override
	public Set<StreamCapability> getCapabilities() {
		return consumer.getCapabilities();
	}

	@Override
	public void close(@NotNull Throwable e) {
		consumer.close(e);
	}
}
