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

import static io.datakernel.jmx.Utils.filterNulls;

public final class JmxReducers {
	private JmxReducers() {}

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

	public static final class JmxReducerSum implements JmxReducer<Number> {

		@Override
		public Number reduce(List<? extends Number> input) {
			List<? extends Number> inputListWithoutNulls = filterNulls(input);

			if (inputListWithoutNulls.size() == 0) {
				return null;
			}

			Number first = inputListWithoutNulls.get(0);
			if (isFloatingPointNumber(first)) {
				double floatingPointSum = first.doubleValue();
				for (int i = 1; i < inputListWithoutNulls.size(); i++) {
					floatingPointSum += inputListWithoutNulls.get(i).doubleValue();

				}
				return floatingPointSum;
			} else if (isIntegerNumber(first)) {
				long integerSum = first.longValue();
				for (int i = 1; i < inputListWithoutNulls.size(); i++) {
					integerSum += inputListWithoutNulls.get(i).longValue();
				}
				return integerSum;
			} else {
				throw new IllegalArgumentException(
						"Cannot calculate sum of objects of type: " + first.getClass().getName());
			}
		}
	}

	public static final class JmxReducerMin implements JmxReducer<Number> {

		@Override
		public Number reduce(List<? extends Number> input) {
			List<? extends Number> inputListWithoutNulls = filterNulls(input);

			if (inputListWithoutNulls.size() == 0) {
				return null;
			}

			Number first = inputListWithoutNulls.get(0);
			if (isFloatingPointNumber(first)) {
				double floatingPointMin = first.doubleValue();
				for (int i = 1; i < inputListWithoutNulls.size(); i++) {
					double currentValue = inputListWithoutNulls.get(i).doubleValue();
					if (currentValue < floatingPointMin) {
						floatingPointMin = currentValue;
					}
				}
				return floatingPointMin;
			} else if (isIntegerNumber(first)) {
				long integerMin = first.longValue();
				for (int i = 1; i < inputListWithoutNulls.size(); i++) {
					long currentValue = inputListWithoutNulls.get(i).longValue();
					if (currentValue < integerMin) {
						integerMin = currentValue;
					}
				}
				return integerMin;
			} else {
				throw new IllegalArgumentException(
						"Cannot calculate sum of objects of type: " + first.getClass().getName());
			}
		}
	}

	public static final class JmxReducerMax implements JmxReducer<Number> {

		@Override
		public Number reduce(List<? extends Number> input) {
			List<? extends Number> inputListWithoutNulls = filterNulls(input);

			if (inputListWithoutNulls.size() == 0) {
				return null;
			}

			Number first = inputListWithoutNulls.get(0);
			if (isFloatingPointNumber(first)) {
				double floatingPointMax = first.doubleValue();
				for (int i = 1; i < inputListWithoutNulls.size(); i++) {
					double currentValue = inputListWithoutNulls.get(i).doubleValue();
					if (currentValue > floatingPointMax) {
						floatingPointMax = currentValue;
					}
				}
				return floatingPointMax;
			} else if (isIntegerNumber(first)) {
				long integerMax = first.longValue();
				for (int i = 1; i < inputListWithoutNulls.size(); i++) {
					long currentValue = inputListWithoutNulls.get(i).longValue();
					if (currentValue > integerMax) {
						integerMax = currentValue;
					}
				}
				return integerMax;
			} else {
				throw new IllegalArgumentException(
						"Cannot calculate sum of objects of type: " + first.getClass().getName());
			}
		}
	}

	private static boolean isFloatingPointNumber(Number number) {
		Class<?> numberClass = number.getClass();
		return Float.class.isAssignableFrom(numberClass) || Double.class.isAssignableFrom(numberClass);
	}

	private static boolean isIntegerNumber(Number number) {
		Class<?> numberClass = number.getClass();
		return Byte.class.isAssignableFrom(numberClass) || Short.class.isAssignableFrom(numberClass)
				|| Integer.class.isAssignableFrom(numberClass) || Long.class.isAssignableFrom(numberClass);
	}
}
