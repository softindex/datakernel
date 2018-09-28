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

package io.datakernel.serial;

import io.datakernel.async.AsyncProcess;
import io.datakernel.async.MaterializedStage;
import io.datakernel.async.Stage;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public interface SerialOutput<T> {
	void setOutput(SerialConsumer<T> output);

	default SerialSupplier<T> getOutputSupplier() {
		return getOutputSupplier(new SerialZeroBuffer<>());
	}

	default SerialSupplier<T> getOutputSupplier(SerialQueue<T> queue) {
		setOutput(queue.getConsumer());
		return queue.getSupplier();
	}

	default void streamTo(SerialInput<T> to) {
		streamTo(to, new SerialZeroBuffer<>());
	}

	default void streamTo(SerialInput<T> to, SerialQueue<T> queue) {
		MaterializedStage<Void> extraAcknowledgement = to.setInput(queue.getSupplier());
		this.setOutput(queue.getConsumer().withAcknowledgement(ack -> ack.both(extraAcknowledgement)));
		if (this instanceof AsyncProcess) {
			getCurrentEventloop().post(((AsyncProcess) this)::start);
		}
		if (to instanceof AsyncProcess) {
			getCurrentEventloop().post(((AsyncProcess) to)::start);
		}
	}

	default Stage<Void> streamTo(SerialConsumer<T> to) {
		return streamTo(to, new SerialZeroBuffer<>());
	}

	default Stage<Void> streamTo(SerialConsumer<T> to, SerialQueue<T> queue) {
		this.setOutput(queue.getConsumer());
		Stage<Void> result = queue.getSupplier().streamTo(to);
		if (this instanceof AsyncProcess) {
			((AsyncProcess) this).start();
		}
		return result;
	}

}
