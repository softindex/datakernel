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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;

public final class JmxAccumulators {
	// TODO (vmykhalko): add rule that all attributes of accumulator should be JMX compatible ? nope

	private JmxAccumulators() {}

	public static JmxAccumulator<Object> getEquivalenceAccumulator() {
		return new JmxAccumulator<Object>() {
			private boolean initialized = false;
			private Object value = null;
			private boolean allValuesAreSame = true;

			@Override
			public void add(Object value) {
				if (allValuesAreSame) {
					if (!initialized) {
						this.initialized = true;
						this.value = value;
					} else {
						allValuesAreSame = Objects.equals(this.value, value);
					}
				}
			}

			@JmxAttribute(skipName = true)
			public Object getValue() throws AggregationException {
				if (!allValuesAreSame) {
					throw new AggregationException();
				}
				return value;
			}
		};
	}

	public static JmxAccumulator<List<?>> getListAccumulator() {
		return new JmxAccumulator<List<?>>() {
			private final List<Object> listAccum = new ArrayList<>();

			@Override
			public void add(List<?> list) {
				if (list != null) {
					listAccum.addAll(list);
				}
			}

			@JmxAttribute(skipName = true)
			public List<Object> getList() {
				return listAccum;
			}
		};
	}

	public static JmxAccumulator<Object[]> getArrayAccumulator() {
		return new JmxAccumulator<Object[]>() {
			private final List<Object> listAccum = new ArrayList<>();

			@Override
			public void add(Object[] array) {
				if (array != null) {
					listAccum.addAll(asList(array));
				}
			}

			@JmxAttribute(skipName = true)
			public Object[] getArray() {
				return listAccum.toArray(new Object[listAccum.size()]);
			}
		};
	}

	@SuppressWarnings("unchecked")
	public static <T extends JmxStats<T>> JmxAccumulator<T> getJmxStatsAccumulatorFor(JmxStats<T> jmxStats)
			throws IllegalAccessException, InstantiationException {
		return (JmxStats<T>) jmxStats.getClass().newInstance();
	}

//	public static Jmx

//	// TODO(vmykhalko): refactor considering Utils
//	private static boolean isSimpleType(Class<?> clazz) {
//		return boolean.class.isAssignableFrom(clazz)
//				|| byte.class.isAssignableFrom(clazz)
//				|| short.class.isAssignableFrom(clazz)
//				|| char.class.isAssignableFrom(clazz)
//				|| int.class.isAssignableFrom(clazz)
//				|| long.class.isAssignableFrom(clazz)
//				|| float.class.isAssignableFrom(clazz)
//				|| double.class.isAssignableFrom(clazz);
//	}
}
