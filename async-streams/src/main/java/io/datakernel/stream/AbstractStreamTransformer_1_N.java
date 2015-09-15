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

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a {@link AbstractStreamConsumer} with several {@link AbstractStreamProducer} .
 *
 * @param <I> type of receiving items
 */
@SuppressWarnings("unchecked")
public abstract class AbstractStreamTransformer_1_N<I> extends AbstractStreamConsumer<I> {
	protected List<AbstractStreamProducer<?>> outputs = new ArrayList<>();

	protected void onDataReceiverChanged(int outputIndex) {
	}

	/**
	 * Creates a new instance of this object
	 *
	 * @param eventloop event loop in which this consumer will run
	 */
	public AbstractStreamTransformer_1_N(Eventloop eventloop) {
		super(eventloop);
	}

	protected <T extends AbstractStreamProducer<?>> T addOutput(T newOutput) {
		outputs.add(newOutput);
		return newOutput;
	}

	@Override
	public void onProducerError(Exception e) {
		upstreamProducer.onConsumerError(e);
		closeDownstreamsWithError(e);
	}

	/**
	 * Returns all producers from this consumer
	 */
	public int getOutputsCount() {
		return outputs.size();
	}

	/**
	 * Checks if all producers of this consumer are ready
	 *
	 * @return true if all are ready, false else
	 */
	protected boolean allOutputsResumed() {
		for (AbstractStreamProducer<?> output : outputs) {
			if (output.getDownstream() == null)
				return false;
			if (output.getStatus() != AbstractStreamProducer.READY) {
				return false;
			}
		}
		return true;
	}

	protected void sendEndOfStreamToDownstreams() {
		for (AbstractStreamProducer<?> output : outputs) {
			output.sendEndOfStream();
		}
	}

	protected void closeDownstreamsWithError(Exception e) {
		for (AbstractStreamProducer<?> output : outputs) {
			output.onConsumerError(e);
		}
	}
}
