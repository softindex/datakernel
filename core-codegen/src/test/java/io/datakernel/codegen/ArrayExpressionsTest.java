package io.datakernel.codegen;

import org.junit.Test;

import java.lang.reflect.Array;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.common.collection.CollectionUtils.map;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public final class ArrayExpressionsTest {
	@Test
	public void testArrayAndMultiArrayOfObjects() {
		ArraysOfObjects arrays = (ArraysOfObjects) createArrayTestObject(Integer.class);
		Integer[] array1 = arrays.getArray1();
		Integer[][] array2 = arrays.getArray2();
		Integer[][][] array3 = arrays.getArray3();

		assertArrayEquals(new Integer[]{10, 11, 12, 13, 14, 15}, array1);
		assertArrayEquals(new Integer[][]{{10, 11}, {12, 13}, {14, 15}}, array2);
		assertArrayEquals(new Integer[][][]{{{10}, {11}}, {{12}, {13}}, {{14}, {15}}}, array3);

		assertEquals((Integer) 13, arrays.getElement(array1));
		assertEquals((Integer) 13, arrays.getElement(array2));
		assertEquals((Integer) 13, arrays.getElement(array3));

		assertArrayEquals(new Integer[]{12, 13}, arrays.getArray(array2, 1));
		assertArrayEquals(new Integer[]{13}, arrays.getArray(array3, 1, 1));
	}

	@Test
	public void testArrayAndMultiArrayOfPrimitives() {
		ArraysOfPrimitives arrays = (ArraysOfPrimitives) createArrayTestObject(int.class);
		int[] array1 = arrays.getArray1();
		int[][] array2 = arrays.getArray2();
		int[][][] array3 = arrays.getArray3();

		assertArrayEquals(new int[]{10, 11, 12, 13, 14, 15}, array1);
		assertArrayEquals(new int[][]{{10, 11}, {12, 13}, {14, 15}}, array2);
		assertArrayEquals(new int[][][]{{{10}, {11}}, {{12}, {13}}, {{14}, {15}}}, array3);

		assertEquals(13, arrays.getElement(array1));
		assertEquals(13, arrays.getElement(array2));
		assertEquals(13, arrays.getElement(array3));

		assertArrayEquals(new int[]{12, 13}, arrays.getArray(array2, 1));
		assertArrayEquals(new int[]{13}, arrays.getArray(array3, 1, 1));
	}

	@Test
	public void testMultidimensionalWithUnspecifiedLengths() throws ReflectiveOperationException {
		Expression create = switchByKey(length(arg(1)), map(
				value(0), multiArrayNew(Integer[][][][].class, arg(0)),
				value(1), multiArrayNew(Integer[][][][].class,
						arg(0),
						arrayGet(arg(1), value(0))),
				value(2), multiArrayNew(Integer[][][][].class,
						arg(0),
						arrayGet(arg(1), value(0)),
						arrayGet(arg(1), value(1))),
				value(3), multiArrayNew(Integer[][][][].class,
						arg(0),
						arrayGet(arg(1), value(0)),
						arrayGet(arg(1), value(1)),
						arrayGet(arg(1), value(2)))
		));

		Expression get = switchByKey(length(arg(2)), map(
				value(0), multiArrayGet(arg(0), arg(1)),
				value(1), multiArrayGet(arg(0), arg(1), arrayGet(arg(2), value(0))),
				value(2), multiArrayGet(arg(0), arg(1), arrayGet(arg(2), value(0)), arrayGet(arg(2), value(1))),
				value(3), multiArrayGet(arg(0), arg(1), arrayGet(arg(2), value(0)), arrayGet(arg(2), value(1)), arrayGet(arg(2), value(2)))
		));

		Expression set = switchByKey(length(arg(3)), map(
				value(0), multiArraySet(arg(0), singletonList(arg(2)), arg(1)),
				value(1), multiArraySet(arg(0), asList(arg(2), arrayGet(arg(3), value(0))), arg(1)),
				value(2), multiArraySet(arg(0), asList(arg(2), arrayGet(arg(3), value(0)), arrayGet(arg(3), value(1))), arg(1)),
				value(3), multiArraySet(arg(0), asList(arg(2), arrayGet(arg(3), value(0)), arrayGet(arg(3), value(1)), arrayGet(arg(3), value(2))), arg(1))
		));

		UnspecifiedLengths unspecifiedLengths = ClassBuilder.create(DefiningClassLoader.create(), UnspecifiedLengths.class)
				.withMethod("create", create)
				.withMethod("get", get)
				.withMethod("set", set)
				.build().getConstructor().newInstance();

		assertArrayEquals(new Integer[3], unspecifiedLengths.create(3));
		assertArrayEquals(new Integer[3][4], unspecifiedLengths.create(3, 4));
		assertArrayEquals(new Integer[3][4][5], unspecifiedLengths.create(3, 4, 5));
		assertArrayEquals(new Integer[3][4][5][6], unspecifiedLengths.create(3, 4, 5, 6));

		Integer[][][][] array = unspecifiedLengths.create(3, 4);

		assertArrayEquals(new Integer[4][][], (Integer[][][]) unspecifiedLengths.get(array, 0));
		assertNull(unspecifiedLengths.get(array, 0, 0));

		Integer[][] integers2 = new Integer[1][2];
		unspecifiedLengths.set(array, integers2, 0, 0);
		assertSame(integers2, unspecifiedLengths.get(array, 0, 0));
	}

	public interface UnspecifiedLengths {
		Integer[][][][] create(int length1, int... otherLengths);

		Object get(Integer[][][][] array, int index1, int... indexes);

		void set(Integer[][][][] array, Object value, int index1, int... indexes);
	}

	public interface ArraysOfObjects {
		Integer[] getArray1();

		Integer[][] getArray2();

		Integer[][][] getArray3();

		Integer getElement(Integer[] array);

		Integer getElement(Integer[][] array);

		Integer getElement(Integer[][][] array);

		Integer[] getArray(Integer[][] array, int index);

		Integer[] getArray(Integer[][][] array, int index1, int index2);
	}

	public interface ArraysOfPrimitives {
		int[] getArray1();

		int[][] getArray2();

		int[][][] getArray3();

		int getElement(int[] array);

		int getElement(int[][] array);

		int getElement(int[][][] array);

		int[] getArray(int[][] array, int index);

		int[] getArray(int[][][] array, int index1, int index2);
	}

	private Object createArrayTestObject(Class<Integer> cls) {
		Class<?> arrayCls = Array.newInstance(cls, new int[1]).getClass();  // int[].class or Integer[].class
		Class<?> array2Cls = Array.newInstance(cls, new int[2]).getClass(); // int[][].class or Integer[][].class
		Class<?> array3Cls = Array.newInstance(cls, new int[3]).getClass(); // int[][][].class or Integer[][][].class

		Expression getArray1Expr = let(arrayNew(arrayCls, value(6)), variable ->
				sequence(arraySet(variable, value(0), getArrayValue(cls, 10)),
						arraySet(variable, value(1), getArrayValue(cls, 11)),
						arraySet(variable, value(2), getArrayValue(cls, 12)),
						arraySet(variable, value(3), getArrayValue(cls, 13)),
						arraySet(variable, value(4), getArrayValue(cls, 14)),
						arraySet(variable, value(5), getArrayValue(cls, 15)),
						variable));

		Expression getArray2Expr = let(
				multiArrayNew(array2Cls, value(3), value(2)),
				variable -> sequence(
						multiArraySet(variable, asList(value(0), value(0)), getArrayValue(cls, 10)),
						multiArraySet(variable, asList(value(0), value(1)), getArrayValue(cls, 11)),
						multiArraySet(variable, asList(value(1), value(0)), getArrayValue(cls, 12)),
						multiArraySet(variable, asList(value(1), value(1)), getArrayValue(cls, 13)),
						multiArraySet(variable, asList(value(2), value(0)), getArrayValue(cls, 14)),
						multiArraySet(variable, asList(value(2), value(1)), getArrayValue(cls, 15)),
						variable)
		);

		Expression getArray3Expr = let(
				multiArrayNew(array3Cls, value(3), value(2), value(1)),
				variable -> sequence(
						multiArraySet(variable, asList(value(0), value(0), value(0)), getArrayValue(cls, 10)),
						multiArraySet(variable, asList(value(0), value(1), value(0)), getArrayValue(cls, 11)),
						multiArraySet(variable, asList(value(1), value(0), value(0)), getArrayValue(cls, 12)),
						multiArraySet(variable, asList(value(1), value(1), value(0)), getArrayValue(cls, 13)),
						multiArraySet(variable, asList(value(2), value(0), value(0)), getArrayValue(cls, 14)),
						multiArraySet(variable, asList(value(2), value(1), value(0)), getArrayValue(cls, 15)),
						variable)
		);

		Expression getElement1 = arrayGet(arg(0), value(3));
		Expression getElement2 = multiArrayGet(arg(0), value(1), value(1));
		Expression getElement3 = multiArrayGet(arg(0), value(1), value(1), value(0));

		Expression getArray1 = arrayGet(arg(0), arg(1));
		Expression getArray2 = multiArrayGet(arg(0), arg(1), arg(2));

		ClassBuilder<?> classBuilder = ClassBuilder.create(DefiningClassLoader.create(),
				cls.isPrimitive() ? ArraysOfPrimitives.class : ArraysOfObjects.class)
				.withMethod("getArray1", getArray1Expr)
				.withMethod("getArray2", getArray2Expr)
				.withMethod("getArray3", getArray3Expr)
				.withMethod("getElement", cls, singletonList(arrayCls), getElement1)
				.withMethod("getElement", cls, singletonList(array2Cls), getElement2)
				.withMethod("getElement", cls, singletonList(array3Cls), getElement3)
				.withMethod("getArray", arrayCls, asList(array2Cls, int.class), getArray1)
				.withMethod("getArray", arrayCls, asList(array3Cls, int.class, int.class), getArray2);

		try {
			return classBuilder.build().newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new AssertionError(e);
		}
	}

	private static Expression getArrayValue(Class<?> cls, int val) {
		Expression value = value(val);
		return cls.isPrimitive() ? value : cast(value, Integer.class);
	}
}
