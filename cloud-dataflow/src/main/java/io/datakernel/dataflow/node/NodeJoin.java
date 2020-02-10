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

package io.datakernel.dataflow.node;

import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.dataflow.graph.TaskContext;
import io.datakernel.datastream.processor.StreamJoin;
import io.datakernel.datastream.processor.StreamJoin.Joiner;

import java.util.Collection;
import java.util.Comparator;
import java.util.function.Function;

import static java.util.Collections.singletonList;

/**
 * Represents a node, which joins two internalConsumers streams (left and right) into one, based on logic, defined by key functions and joiner.
 *
 * @param <K> keys type
 * @param <L> left stream data type
 * @param <R> right stream data type
 * @param <V> output stream data type
 */
public final class NodeJoin<K, L, R, V> implements Node {
	private final StreamId left;
	private final StreamId right;
	private final StreamId output;
	private final Comparator<K> keyComparator;
	private final Function<L, K> leftKeyFunction;
	private final Function<R, K> rightKeyFunction;
	private final Joiner<K, L, R, V> joiner;

	public NodeJoin(StreamId left, StreamId right,
			Comparator<K> keyComparator, Function<L, K> leftKeyFunction, Function<R, K> rightKeyFunction,
			Joiner<K, L, R, V> joiner) {
		this(left, right, new StreamId(), keyComparator, leftKeyFunction, rightKeyFunction, joiner);
	}

	public NodeJoin(StreamId left, StreamId right, StreamId output,
			Comparator<K> keyComparator, Function<L, K> leftKeyFunction, Function<R, K> rightKeyFunction,
			Joiner<K, L, R, V> joiner) {
		this.left = left;
		this.right = right;
		this.output = output;
		this.keyComparator = keyComparator;
		this.leftKeyFunction = leftKeyFunction;
		this.rightKeyFunction = rightKeyFunction;
		this.joiner = joiner;
	}

	@Override
	public Collection<StreamId> getOutputs() {
		return singletonList(output);
	}

	@Override
	public void createAndBind(TaskContext taskContext) {
		StreamJoin<K, L, R, V> join =
				StreamJoin.create(keyComparator, leftKeyFunction, rightKeyFunction, joiner);
		taskContext.export(output, join.getOutput());
		taskContext.bindChannel(left, join.getLeft());
		taskContext.bindChannel(right, join.getRight());
	}

	public StreamId getLeft() {
		return left;
	}

	public StreamId getRight() {
		return right;
	}

	public StreamId getOutput() {
		return output;
	}

	public Comparator<K> getKeyComparator() {
		return keyComparator;
	}

	public Function<L, K> getLeftKeyFunction() {
		return leftKeyFunction;
	}

	public Function<R, K> getRightKeyFunction() {
		return rightKeyFunction;
	}

	public Joiner<K, L, R, V> getJoiner() {
		return joiner;
	}
}
