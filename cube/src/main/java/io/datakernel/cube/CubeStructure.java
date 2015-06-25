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

package io.datakernel.cube;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import io.datakernel.asm.DefiningClassLoader;
import io.datakernel.codegen.AsmFunctionFactory;
import io.datakernel.codegen.FunctionDefComparator;
import io.datakernel.codegen.FunctionDefSequence;
import io.datakernel.cube.dimensiontype.DimensionType;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerFactory;
import io.datakernel.serializer.asm.SerializerGenClass;
import io.datakernel.stream.processor.StreamReducers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.codegen.FunctionDefs.*;

/**
 * Defines a structure of a cube.
 * Cube structure is defined by dimensions, measures and their types.
 * Contains methods for defining dynamic classes, that are used for different cube operations.
 * Provides serializer for records that have the defined structure.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class CubeStructure {
	private final DefiningClassLoader classLoader;
	private final Map<String, DimensionType> dimensions;
	private final Map<String, MeasureType> measures;
	private final SerializerFactory bufferSerializerFactory;

	private final static Logger logger = LoggerFactory.getLogger(CubeStructure.class);

	/**
	 * Constructs a new cube structure with the given class loader, dimensions and measures.
	 *
	 * @param classLoader class loader for loading dynamically created classes
	 * @param dimensions  map of cube dimensions (key is dimension name)
	 * @param measures    map of cube measures (key is measure name)
	 */
	public CubeStructure(DefiningClassLoader classLoader,
	                     Map<String, DimensionType> dimensions, Map<String, MeasureType> measures) {
		this.classLoader = classLoader;
		this.dimensions = dimensions;
		this.measures = measures;
		this.bufferSerializerFactory = SerializerFactory.createBufferSerializerFactory(classLoader, true, true);
	}

	public void checkThatDimensionsExist(List<String> dimensions) {
		for (String dimension : dimensions) {
			if (!this.dimensions.containsKey(dimension)) {
				throw new CubeException("Dimension with the name '" + dimension + "' not found.");
			}
		}
	}

	public void checkThatMeasuresExist(List<String> measures) {
		for (String measure : measures) {
			if (!this.measures.containsKey(measure)) {
				throw new CubeException("Measure with the name '" + measure + "' not found.");
			}
		}
	}

	public Map<String, DimensionType> getDimensions() {
		return dimensions;
	}

	public DimensionType getDimensionType(String dimension) {
		return dimensions.get(dimension);
	}

	public Map<String, MeasureType> getMeasures() {
		return measures;
	}

	public Class<?> createKeyClass(List<String> dimensions) {
		logger.trace("Creating key class for dimensions {}", dimensions);
		AsmFunctionFactory factory = new AsmFunctionFactory(classLoader, Comparable.class);
		for (String dimension : dimensions) {
			DimensionType d = this.dimensions.get(dimension);
			factory.field(dimension, d.getDataType());
		}
		factory.method("compareTo", compareTo(dimensions));
		factory.method("equals", asEquals(dimensions));
		factory.method("hashCode", hashCodeOfThis(dimensions));
		factory.method("toString", asString(dimensions));

		return factory.defineClass();
	}

	public Function createFieldFunction(Class<?> recordClass, Class<?> fieldClass, List<String> fields) {
		logger.trace("Creating field function for fields {}", fields);
		AsmFunctionFactory factory = new AsmFunctionFactory(classLoader, Function.class);
		FunctionDefSequence applyDef = sequence(let("FIELD", constructor(fieldClass)));

		for (String field : fields) {
			applyDef.add(set(
					field(var("FIELD"), field),
					field(cast(arg(0), recordClass), field)));
		}

		applyDef.add(var("FIELD"));
		factory.method("apply", applyDef);

		return (Function) factory.newInstance();
	}

	public Class<?> createFieldClass(List<String> fields) {
		logger.trace("Creating field class for fields {}", fields);
		AsmFunctionFactory factory = new AsmFunctionFactory(classLoader, Comparable.class);

		for (String field : fields) {
			Class<?> dataType = null;
			MeasureType measureType = this.measures.get(field);

			if (measureType != null) {
				dataType = measureType.getDataType();
			} else {
				DimensionType dimensionType = this.dimensions.get(field);
				if (dimensionType != null) {
					dataType = dimensionType.getDataType();
				}
			}

			factory.field(field, dataType);
		}

		factory.method("compareTo", compareTo(fields));
		factory.method("equals", asEquals(fields));
		factory.method("toString", asString(fields));
		factory.method("hashCode", hashCodeOfThis(fields));

		return factory.defineClass();
	}

	public Comparator createFieldComparator(CubeQuery query, Class<?> fieldClass) {
		logger.trace("Creating field comparator for query {}", query.toString());
		AsmFunctionFactory factory = new AsmFunctionFactory(classLoader, Comparator.class);
		FunctionDefComparator comparator = comparator();
		List<CubeQuery.CubeOrdering> orderings = query.getOrderings();

		for (CubeQuery.CubeOrdering ordering : orderings) {
			boolean isAsc = ordering.isAsc();
			String field = ordering.getField();
			if (isAsc)
				comparator.add(
						field(cast(arg(0), fieldClass), field),
						field(cast(arg(1), fieldClass), field));
			else
				comparator.add(
						field(cast(arg(1), fieldClass), field),
						field(cast(arg(0), fieldClass), field));
		}

		factory.method("compare", comparator);

		return (Comparator) factory.newInstance();
	}

	public Function createKeyFunction(Class<?> recordClass, Class<?> keyClass, List<String> dimensions) {
		logger.trace("Creating key function for dimensions {}", dimensions);
		AsmFunctionFactory factory = new AsmFunctionFactory<>(classLoader, Function.class);
		FunctionDefSequence applyDef = sequence(let("KEY", constructor(keyClass)));
		for (String dimension : dimensions) {
			applyDef.add(set(
					field(var("KEY"), dimension),
					field(cast(arg(0), recordClass), dimension)));
		}
		applyDef.add(var("KEY"));
		factory.method("apply", applyDef);
		return (Function) factory.newInstance();
	}

	public Class<?> createRecordClass(List<String> dimensions, List<String> measures) {
		logger.trace("Creating record class for dimensions {}, measures {}", dimensions, measures);
		AsmFunctionFactory<Object> factory = new AsmFunctionFactory<>(classLoader, Object.class);
		for (String dimension : dimensions) {
			DimensionType d = this.dimensions.get(dimension);
			factory.field(dimension, d.getDataType());
		}
		for (String measure : measures) {
			MeasureType m = this.measures.get(measure);
			factory.field(measure, m.getDataType());
		}
		factory.method("toString", asString(newArrayList(concat(dimensions, measures))));
		return factory.defineClass();
	}

	public Class<?> createResultClass(CubeQuery query) {
		logger.trace("Creating result class for query {}", query.toString());
		AsmFunctionFactory<Object> factory = new AsmFunctionFactory<>(classLoader, Object.class);
		List<String> resultDimensions = query.getResultDimensions();
		List<String> resultMeasures = query.getResultMeasures();
		for (String dimension : resultDimensions) {
			DimensionType d = this.dimensions.get(dimension);
			if (d == null) {
				throw new CubeException("Dimension with the name '" + dimension + "' not found.");
			}
			factory.field(dimension, d.getDataType());
		}
		for (String measure : resultMeasures) {
			MeasureType m = this.measures.get(measure);
			if (m == null) {
				throw new CubeException("Measure with the name '" + measure + "' not found.");
			}
			factory.field(measure, m.getDataType());
		}
		factory.method("toString", asString(newArrayList(concat(resultDimensions, resultMeasures))));
		return factory.defineClass();
	}

	public StreamReducers.Reducer aggregationReducer(Class<?> inputClass, Class<?> outputClass,
	                                                 List<String> recordDimensions, List<String> recordMeasures) {
		logger.trace("Creating aggregation reducer for dimensions {}, measures {}", recordDimensions, recordMeasures);
		AsmFunctionFactory factory = new AsmFunctionFactory(classLoader, StreamReducers.Reducer.class);

		FunctionDefSequence onFirstItemDef = sequence(let("ACCUMULATOR", constructor(outputClass)));
		for (String dimension : recordDimensions) {
			onFirstItemDef.add(set(
					field(var("ACCUMULATOR"), dimension),
					field(cast(arg(2), inputClass), dimension)));
		}
		for (String measure : recordMeasures) {
			onFirstItemDef.add(set(
					field(var("ACCUMULATOR"), measure),
					field(cast(arg(2), inputClass), measure)));
		}
		onFirstItemDef.add(var("ACCUMULATOR"));
		factory.method("onFirstItem", onFirstItemDef);

		FunctionDefSequence onNextItemDef = sequence();
		for (String measure : recordMeasures) {
			onNextItemDef.add(set(
					field(cast(arg(3), outputClass), measure),
					add(
							field(cast(arg(3), outputClass), measure),
							field(cast(arg(2), inputClass), measure))));
		}
		onNextItemDef.add(arg(3));
		factory.method("onNextItem", onNextItemDef);

		factory.method("onComplete", call(arg(0), "onData", arg(2)));

		return (StreamReducers.Reducer) factory.newInstance();
	}

	public StreamReducers.Reducer mergeMeasuresReducer(Class<?> inputClass, Class<?> outputClass,
	                                                   List<String> dimensions, List<String> recordMeasures) {
		logger.trace("Creating merge measures reducer for dimensions {}, measures {}", dimensions, recordMeasures);
		AsmFunctionFactory factory = new AsmFunctionFactory(classLoader, StreamReducers.Reducer.class);

		FunctionDefSequence onFirstItemDef = sequence(let("ACCUMULATOR", constructor(outputClass)));
		for (String dimension : dimensions) {
			onFirstItemDef.add(set(
					field(var("ACCUMULATOR"), dimension),
					field(cast(arg(2), inputClass), dimension)));
		}
		for (String measure : recordMeasures) {
			onFirstItemDef.add(set(
					field(var("ACCUMULATOR"), measure),
					field(cast(arg(2), inputClass), measure)));
		}
		onFirstItemDef.add(var("ACCUMULATOR"));
		factory.method("onFirstItem", onFirstItemDef);

		FunctionDefSequence onNextItemDef = sequence(let("ACCUMULATOR", cast(arg(3), outputClass)));
		for (String measure : recordMeasures) {
			onNextItemDef.add(set(
					field(var("ACCUMULATOR"), measure),
					add(
							field(cast(arg(2), inputClass), measure),
							field(cast(arg(3), outputClass), measure)
					)));
		}
		onNextItemDef.add(var("ACCUMULATOR"));
		factory.method("onNextItem", onNextItemDef);

		factory.method("onComplete", call(arg(0), "onData", arg(2)));

		return (StreamReducers.Reducer) factory.newInstance();
	}

	public Aggregate createAggregate(Class<?> inputClass, Class<?> outputClass,
	                                 List<String> recordDimensions, List<String> recordMeasures) {
		logger.trace("Creating aggregate for dimensions {}, measures {}", recordDimensions, recordMeasures);
		AsmFunctionFactory factory = new AsmFunctionFactory(classLoader, Aggregate.class);

		FunctionDefSequence createAccumulatorDef = sequence(let("RESULT", constructor(outputClass)));
		for (String dimension : recordDimensions) {
			createAccumulatorDef.add(set(
					field(var("RESULT"), dimension),
					field(cast(arg(0), inputClass), dimension)));
		}
		for (String measure : recordMeasures) {
			createAccumulatorDef.add(set(
					field(var("RESULT"), measure),
					field(cast(arg(0), inputClass), measure)));
		}
		createAccumulatorDef.add(var("RESULT"));
		factory.method("createAccumulator", createAccumulatorDef);

		FunctionDefSequence accumulateDef = sequence();
		for (String measure : recordMeasures) {
			accumulateDef.add(set(
					field(cast(arg(0), outputClass), measure),
					add(
							field(cast(arg(0), outputClass), measure),
							field(cast(arg(1), inputClass), measure))));
		}
		factory.method("accumulate", accumulateDef);

		return (Aggregate) factory.newInstance();
	}

	public <T> BufferSerializer<T> createBufferSerializer(Class<T> recordClass, List<String> dimensions, List<String> measures) {
		SerializerGenClass serializerGenClass = new SerializerGenClass(recordClass);
		for (String dimension : dimensions) {
			DimensionType dimensionType = this.dimensions.get(dimension);
			try {
				Field field = recordClass.getField(dimension);
				serializerGenClass.addField(field, dimensionType.serializerGen(), -1, -1);
			} catch (NoSuchFieldException e) {
				throw propagate(e);
			}
		}
		for (String measure : measures) {
			MeasureType measureType = this.measures.get(measure);
			try {
				Field field = recordClass.getField(measure);
				serializerGenClass.addField(field, measureType.serializerGen(), -1, -1);
			} catch (NoSuchFieldException e) {
				throw propagate(e);
			}
		}
		return bufferSerializerFactory.createBufferSerializer(serializerGenClass);
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("dimensions", dimensions)
				.add("measures", measures)
				.toString();
	}
}
