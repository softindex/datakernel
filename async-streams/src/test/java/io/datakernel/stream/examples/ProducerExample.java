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

package io.datakernel.stream.examples;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamProducer;

/**
 * Example 3.
 * Example of creating the custom StreamProducer.
 * This producer just streams all numbers from 0 to number specified in constructor.
 */
public class ProducerExample extends AbstractStreamProducer<Integer> {
	private final int count;
	private int i;

	public ProducerExample(Eventloop eventloop, int count) {
		super(eventloop);
		this.count = count;
	}

	@Override
	protected void doProduce() {
		while (status == READY && i < count) {
			send(i++);
		}
		if (i == count) {
			sendEndOfStream();
		}
	}

	@Override
	protected void onProducerStarted() {
		produce();
	}

	@Override
	public void onResumed() {
		resumeProduce();
	}
}
