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

import java.util.Collection;

/**
 * Defines a node in a single server.
 */
public interface Node {
	/**
	 * Returns a list of ids of outputs of this node.
	 *
	 * @return ids of outputs of this node
	 */
	Collection<StreamId> getOutputs();

	/**
	 * Defines internal node logic and binds it to the task context.
	 *
	 * @param taskContext task context to which certain logic is to be bound
	 */
	void createAndBind(TaskContext taskContext);
}
