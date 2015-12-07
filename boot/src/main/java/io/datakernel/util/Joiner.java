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

package io.datakernel.util;

import java.util.List;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class Joiner {
	private final String separator;

	private Joiner(String separator) {
		this.separator = separator;
	}

	public static Joiner on(char separator) {
		return new Joiner(Character.toString(separator));
	}

	public static Joiner on(String separator) {
		return new Joiner(separator);
	}

	public String join(String[] inputs) {
		checkArgument(inputs != null && inputs.length > 0, "Inputs must contain at least one String");
		StringBuilder stringBuilder = new StringBuilder();
		String first = inputs[0];
		checkNotNull(first);
		stringBuilder.append(inputs[0]);
		for (int i = 1; i < inputs.length; i++) {
			String currentString = checkNotNull(inputs[i]);
			stringBuilder.append(separator);
			stringBuilder.append(currentString);
		}
		return stringBuilder.toString();
	}

	public String join(List<String> inputs) {
		return join(inputs.toArray(new String[inputs.size()]));
	}
}
