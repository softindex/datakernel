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

public class AddOperation implements Operation {
	private final int delta;

	private AddOperation(int delta) {
		this.delta = delta;
	}

	public static AddOperation add(int delta) {
		return new AddOperation(delta);
	}

	@Override
	public int apply(int prev) {
		return prev + delta;
	}

	public int getValue() {
		return delta;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		AddOperation that = (AddOperation) o;

		return delta == that.delta;
	}

	@Override
	public int hashCode() {
		return delta;
	}

	@Override
	public String toString() {
		return (delta > 0 ? "+" : "") + delta;
	}
}
