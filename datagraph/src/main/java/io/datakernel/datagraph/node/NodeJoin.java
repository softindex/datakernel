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

package io.datakernel.datagraph.node;

import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.datagraph.graph.TaskContext;
import io.datakernel.stream.processor.StreamJoin;

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

	private StreamId left;
	private StreamId right;
	private StreamId output;
	private Comparator<K> keyComparator;
	private Function<L, K> leftKeyFunction;
	private Function<R, K> rightKeyFunction;
	private StreamJoin.Joiner<K, L, R, V> joiner;

	public NodeJoin() {}

	public NodeJoin(StreamId left, StreamId right,
					Comparator<K> keyComparator, Function<L, K> leftKeyFunction, Function<R, K> rightKeyFunction,
					StreamJoin.Joiner<K, L, R, V> joiner) {
		this.left = left;
		this.right = right;
		this.output = new StreamId();
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

	public void setLeft(StreamId left) {
		this.left = left;
	}

	public StreamId getRight() {
		return right;
	}

	public void setRight(StreamId right) {
		this.right = right;
	}

	public StreamId getOutput() {
		return output;
	}

	public void setOutput(StreamId output) {
		this.output = output;
	}

	public Comparator<K> getKeyComparator() {
		return keyComparator;
	}

	public void setKeyComparator(Comparator<K> keyComparator) {
		this.keyComparator = keyComparator;
	}

	public Function<L, K> getLeftKeyFunction() {
		return leftKeyFunction;
	}

	public void setLeftKeyFunction(Function<L, K> leftKeyFunction) {
		this.leftKeyFunction = leftKeyFunction;
	}

	public Function<R, K> getRightKeyFunction() {
		return rightKeyFunction;
	}

	public void setRightKeyFunction(Function<R, K> rightKeyFunction) {
		this.rightKeyFunction = rightKeyFunction;
	}

	public StreamJoin.Joiner<K, L, R, V> getJoiner() {
		return joiner;
	}

	public void setJoiner(StreamJoin.Joiner<K, L, R, V> joiner) {
		this.joiner = joiner;
	}
}
