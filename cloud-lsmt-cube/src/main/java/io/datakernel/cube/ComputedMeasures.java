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

package io.datakernel.cube;

import io.datakernel.aggregation.measure.Measure;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Expressions;
import io.datakernel.codegen.utils.Primitives;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

public class ComputedMeasures {
	private static final class E extends Expressions {}

	public static abstract class AbstractComputedMeasure implements ComputedMeasure {
		protected final Set<ComputedMeasure> dependencies;

		public AbstractComputedMeasure(ComputedMeasure... dependencies) {
			this.dependencies = new LinkedHashSet<>(Arrays.asList(dependencies));
		}

		@Nullable
		@Override
		public Class<?> getType(Map<String, Measure> storedMeasures) {
			return null;
		}

		@Override
		public final Set<String> getMeasureDependencies() {
			Set<String> result = new LinkedHashSet<>();
			for (ComputedMeasure dependency : dependencies) {
				result.addAll(dependency.getMeasureDependencies());
			}
			return result;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			AbstractComputedMeasure other = (AbstractComputedMeasure) o;
			return dependencies.equals(other.dependencies);
		}

		@Override
		public int hashCode() {
			return Objects.hash(dependencies);
		}
	}

	private static abstract class AbstractArithmeticMeasure extends AbstractComputedMeasure {
		public AbstractArithmeticMeasure(ComputedMeasure... dependencies) {
			super(dependencies);
		}

		@Override
		public final Class<?> getType(Map<String, Measure> storedMeasures) {
			List<Class<?>> types = new ArrayList<>();
			for (ComputedMeasure dependency : dependencies) {
				types.add(dependency.getType(storedMeasures));
			}
			return E.unifyArithmeticTypes(types);
		}
	}

	public static ComputedMeasure value(Object value) {
		return new ComputedMeasureValue(value);
	}

	public static ComputedMeasure measure(String measureId) {
		return new ComputedMeasureMeasure(measureId);
	}

	public static ComputedMeasure add(ComputedMeasure measure1, ComputedMeasure measure2) {
		return new AbstractArithmeticMeasure(measure1, measure2) {
			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return E.add(measure1.getExpression(record, storedMeasures), measure2.getExpression(record, storedMeasures));
			}
		};
	}

	public static ComputedMeasure sub(ComputedMeasure measure1, ComputedMeasure measure2) {
		return new AbstractArithmeticMeasure(measure1, measure2) {
			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return E.sub(measure1.getExpression(record, storedMeasures), measure2.getExpression(record, storedMeasures));
			}
		};
	}

	public static ComputedMeasure div(ComputedMeasure measure1, ComputedMeasure measure2) {
		return new AbstractComputedMeasure(measure1, measure2) {
			@Override
			public Class<?> getType(Map<String, Measure> storedMeasures) {
				return double.class;
			}

			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				Expression value2 = E.cast(measure2.getExpression(record, storedMeasures), double.class);
				return E.ifThenElse(E.cmpNe(value2, E.value(0.0)),
						E.div(measure1.getExpression(record, storedMeasures), value2),
						E.value(0.0));
			}
		};
	}

	public static ComputedMeasure mul(ComputedMeasure measure1, ComputedMeasure measure2) {
		return new AbstractArithmeticMeasure(measure1, measure2) {
			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return E.mul(measure1.getExpression(record, storedMeasures), measure2.getExpression(record, storedMeasures));
			}
		};
	}

	public static ComputedMeasure sqrt(ComputedMeasure measure) {
		return new AbstractComputedMeasure(measure) {
			@Override
			public Class<?> getType(Map<String, Measure> storedMeasures) {
				return double.class;
			}

			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return E.let(
						E.cast(measure.getExpression(record, storedMeasures), double.class),
						value ->
								E.ifThenElse(E.cmpLe(value, E.value(0.0d)),
										E.value(0.0d),
										E.staticCall(Math.class, "sqrt", value)));
			}
		};
	}

	public static ComputedMeasure sqr(ComputedMeasure measure) {
		return new AbstractComputedMeasure(measure) {
			@Override
			public Class<?> getType(Map<String, Measure> storedMeasures) {
				return double.class;
			}

			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return E.let(E.cast(measure.getExpression(record, storedMeasures), double.class), value ->
						E.mul(value, value));
			}
		};
	}

	public static ComputedMeasure stddev(ComputedMeasure sum, ComputedMeasure sumOfSquares, ComputedMeasure count) {
		return sqrt(variance(sum, sumOfSquares, count));
	}

	public static ComputedMeasure variance(ComputedMeasure sum, ComputedMeasure sumOfSquares, ComputedMeasure count) {
		return sub(div(sumOfSquares, count), sqr(div(sum, count)));
	}

	public static ComputedMeasure percent(ComputedMeasure measure) {
		return mul(measure, value(100));
	}

	public static ComputedMeasure percent(ComputedMeasure numerator, ComputedMeasure denominator) {
		return mul(div(numerator, denominator), value(100));
	}

	private static final class ComputedMeasureValue implements ComputedMeasure {
		private final Object value;

		public ComputedMeasureValue(Object value) {this.value = value;}

		@Override
		public Class<?> getType(Map<String, Measure> storedMeasures) {
			return Primitives.unwrap(value.getClass());
		}

		@Override
		public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
			return E.value(value);
		}

		@Override
		public Set<String> getMeasureDependencies() {
			return emptySet();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			ComputedMeasureValue other = (ComputedMeasureValue) o;
			return Objects.equals(value, other.value);
		}

		@Override
		public int hashCode() {
			return Objects.hash(value);
		}
	}

	private static final class ComputedMeasureMeasure implements ComputedMeasure {
		private final String measureId;

		public ComputedMeasureMeasure(String measureId) {this.measureId = measureId;}

		@Override
		public Class<?> getType(Map<String, Measure> storedMeasures) {
			return (Class<?>) storedMeasures.get(measureId).getFieldType().getDataType();
		}

		@Override
		public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
			return storedMeasures.get(measureId).valueOfAccumulator(E.property(record, measureId));
		}

		@Override
		public Set<String> getMeasureDependencies() {
			return singleton(measureId);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			ComputedMeasureMeasure other = (ComputedMeasureMeasure) o;
			return measureId.equals(other.measureId);
		}

		@Override
		public int hashCode() {
			return Objects.hash(measureId);
		}
	}
}
