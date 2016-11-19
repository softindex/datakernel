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

package io.datakernel.cube;

import io.datakernel.aggregation.AggregationPredicate;
import io.datakernel.aggregation.AggregationPredicates;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logfs.LogCommitTransaction;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.AbstractStreamSplitter;

import java.util.Map;

import static io.datakernel.aggregation.AggregationUtils.scanKeyFields;
import static io.datakernel.aggregation.AggregationUtils.scanMeasureFields;

/**
 * Represents a logic for input records pre-processing and splitting across multiple cube inputs.
 *
 * @param <T> type of input records
 */
public abstract class AggregatorSplitter<T> extends AbstractStreamSplitter<T> {
	public interface Factory<T> {
		AggregatorSplitter<T> create(Eventloop eventloop);
	}

	private Cube cube;
	private LogCommitTransaction<?> transaction;
	private int items;

	public AggregatorSplitter(Eventloop eventloop) {
		super(eventloop);
		this.inputConsumer = new InputConsumer() {
			@Override
			public void onData(T item) {
				++items;
				processItem(item);
			}
		};
	}

	public final StreamProducer<T> newOutput() {
		return addOutput(new OutputProducer<T>());
	}

	protected final <O> StreamDataReceiver<O> addOutput(Class<O> aggregationItemType) {
		return addOutput(aggregationItemType, scanKeyFields(aggregationItemType), scanMeasureFields(aggregationItemType));
	}

	protected final <O> StreamDataReceiver<O> addOutput(Class<O> aggregationItemType, AggregationPredicate predicate) {
		return addOutput(aggregationItemType, scanKeyFields(aggregationItemType), scanMeasureFields(aggregationItemType), predicate);
	}

	protected final <O> StreamDataReceiver<O> addOutput(Class<O> aggregationItemType,
	                                                    Map<String, String> dimensionFields,
	                                                    Map<String, String> measureFields) {
		return addOutput(aggregationItemType, dimensionFields, measureFields, AggregationPredicates.alwaysTrue());
	}

	@SuppressWarnings("unchecked")
	protected final <O> StreamDataReceiver<O> addOutput(Class<O> aggregationItemType,
	                                                    Map<String, String> dimensionFields,
	                                                    Map<String, String> measureFields,
	                                                    AggregationPredicate predicate) {
		StreamProducer streamProducer = newOutput();
		StreamConsumer streamConsumer = cube.consumer(aggregationItemType,
				dimensionFields, measureFields,
				predicate, transaction.addCommitCallback());
		streamProducer.streamTo(streamConsumer);
		return streamConsumer.getDataReceiver();
	}

	protected abstract void addOutputs();

	protected abstract void processItem(T item);

	public final void streamTo(Cube cube, LogCommitTransaction<?> transaction) {
		this.cube = cube;
		this.transaction = transaction;
		addOutputs();
	}

	public int getItems() {
		return items;
	}
}
