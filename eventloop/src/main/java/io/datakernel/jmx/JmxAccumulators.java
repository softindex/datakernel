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

import static io.datakernel.jmx.Utils.*;
import static io.datakernel.util.Preconditions.checkArgument;
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
	public static JmxAccumulator<?> getJmxStatsAccumulatorFor(Class<?> jmxStatsClass)
			throws IllegalAccessException, InstantiationException {
		checkArgument(JmxStats.class.isAssignableFrom(jmxStatsClass));
		return (JmxAccumulator<?>) jmxStatsClass.newInstance();
	}

	public static JmxAccumulator<?> getDefaultAccumulatorFor(Class<?> clazz) {
		if (isPrimitiveType(clazz) || isPrimitiveTypeWrapper(clazz) || isString(clazz) || isThrowable(clazz)) {
			return getEquivalenceAccumulator();
		} else if (isArray(clazz)) {
			return getArrayAccumulator();
		} else if (isList(clazz)) {
			return getListAccumulator();
		}
		throw new RuntimeException("There is no accumulator for " + clazz.getName());
	}

	// default accumulators

	public static JmxAccumulator<Boolean> defaultBooleanAccumulator() {
		return new JmxAccumulator<Boolean>() {
			private boolean initialized = false;
			private Boolean value = null;
			private boolean allValuesAreSame = true;

			@Override
			public void add(Boolean value) {
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
			public Boolean getValue() throws AggregationException{
				if (!allValuesAreSame) {
					throw new AggregationException();
				}
				return value;
			}
		};
	}

	public static JmxAccumulator<Byte> defaultByteAccumulator() {
		return new JmxAccumulator<Byte>() {
			private boolean initialized = false;
			private Byte value = null;
			private boolean allValuesAreSame = true;

			@Override
			public void add(Byte value) {
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
			public Byte getValue() throws AggregationException{
				if (!allValuesAreSame) {
					throw new AggregationException();
				}
				return value;
			}
		};
	}

	public static JmxAccumulator<Short> defaultShortAccumulator() {
		return new JmxAccumulator<Short>() {
			private boolean initialized = false;
			private Short value = null;
			private boolean allValuesAreSame = true;

			@Override
			public void add(Short value) {
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
			public Short getValue() throws AggregationException{
				if (!allValuesAreSame) {
					throw new AggregationException();
				}
				return value;
			}
		};
	}

	public static JmxAccumulator<Character> defaultCharacterAccumulator() {
		return new JmxAccumulator<Character>() {
			private boolean initialized = false;
			private Character value = null;
			private boolean allValuesAreSame = true;

			@Override
			public void add(Character value) {
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
			public Character getValue() throws AggregationException{
				if (!allValuesAreSame) {
					throw new AggregationException();
				}
				return value;
			}
		};
	}

	public static JmxAccumulator<Integer> defaultIntegerAccumulator() {
		return new JmxAccumulator<Integer>() {
			private boolean initialized = false;
			private Integer value = null;
			private boolean allValuesAreSame = true;

			@Override
			public void add(Integer value) {
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
			public Integer getValue() throws AggregationException{
				if (!allValuesAreSame) {
					throw new AggregationException();
				}
				return value;
			}
		};
	}

	public static JmxAccumulator<Long> defaultLongAccumulator() {
		return new JmxAccumulator<Long>() {
			private boolean initialized = false;
			private Long value = null;
			private boolean allValuesAreSame = true;

			@Override
			public void add(Long value) {
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
			public Long getValue() throws AggregationException{
				if (!allValuesAreSame) {
					throw new AggregationException();
				}
				return value;
			}
		};
	}

	public static JmxAccumulator<Float> defaultFloatAccumulator() {
		return new JmxAccumulator<Float>() {
			private boolean initialized = false;
			private Float value = null;
			private boolean allValuesAreSame = true;

			@Override
			public void add(Float value) {
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
			public Float getValue() throws AggregationException{
				if (!allValuesAreSame) {
					throw new AggregationException();
				}
				return value;
			}
		};
	}

	public static JmxAccumulator<Double> defaultDoubleAccumulator() {
		return new JmxAccumulator<Double>() {
			private boolean initialized = false;
			private Double value = null;
			private boolean allValuesAreSame = true;

			@Override
			public void add(Double value) {
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
			public Double getValue() throws AggregationException{
				if (!allValuesAreSame) {
					throw new AggregationException();
				}
				return value;
			}
		};
	}

	public static JmxAccumulator<String> defaultStringAccumulator() {
		return new JmxAccumulator<String>() {
			private boolean initialized = false;
			private String value = null;
			private boolean allValuesAreSame = true;

			@Override
			public void add(String value) {
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
			public String getValue() throws AggregationException{
				if (!allValuesAreSame) {
					throw new AggregationException();
				}
				return value;
			}
		};
	}
}
