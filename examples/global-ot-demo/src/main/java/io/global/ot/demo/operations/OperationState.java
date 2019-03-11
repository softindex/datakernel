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

package io.global.ot.demo.operations;

import io.datakernel.ot.OTState;

public class OperationState implements OTState<Operation> {
	private int counter;

	public OperationState() {
	}

	@Override
	public void init() {
		counter = 0;
	}

	@Override
	public void apply(Operation op) {
		counter = op.apply(counter);
	}

	public int getCounter() {
		return counter;
	}

	@Override
	public String toString() {
		return String.valueOf(counter);
	}
}
