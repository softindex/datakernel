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

package io.datakernel.jmx;

import java.util.List;
import java.util.Objects;

public final class JmxReducers {
	private JmxReducers() {}

	public static JmxReducer<Object> distinct() {
		return new JmxReducerDistinct();
	}

	public static final class JmxReducerDistinct implements JmxReducer<Object> {
		@Override
		public Object reduce(List<?> input) {
			if (input.size() == 0) {
				return null;
			}

			Object firstValue = input.get(0);
			for (int i = 1; i < input.size(); i++) {
				Object currentValue = input.get(i);
				if (!Objects.equals(firstValue, currentValue)) {
					return null;
				}
			}
			return firstValue;
		}
	}
}
