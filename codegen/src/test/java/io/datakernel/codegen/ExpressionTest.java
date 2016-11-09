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

package io.datakernel.codegen;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static io.datakernel.codegen.ExpressionComparator.leftField;
import static io.datakernel.codegen.ExpressionComparator.rightField;
import static io.datakernel.codegen.Expressions.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class ExpressionTest {
	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public static class TestPojo {
		public int field1;
		public int field2;

		public TestPojo(int field1, int field2) {
			this.field1 = field1;
			this.field2 = field2;
		}

		public TestPojo(int field1) {
			this.field1 = field1;
		}

		public void setField1(int field1) {
			this.field1 = field1;
		}

		public int getField1() {
			return field1;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestPojo testPojo = (TestPojo) o;

			return (field1 == testPojo.field1) && (field2 == testPojo.field2);
		}

		@Override
		public int hashCode() {
			int result = field1;
			result = 31 * result + field2;
			return result;
		}

		@Override
		public String toString() {
			return "TestPojo{field1=" + field1 + ", field2=" + field2 + '}';
		}
	}

	public static class TestPojo2 {
		public String field1;
		public int field2;
		public long field3;
		public float field4;
		public int field5;
		public double field6;
		public String field7;

		public TestPojo2(String field1, int field2, long field3, float field4, int field5, double field6, String field7) {
			this.field1 = field1;
			this.field2 = field2;
			this.field3 = field3;
			this.field4 = field4;
			this.field5 = field5;
			this.field6 = field6;
			this.field7 = field7;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestPojo2 testPojo2 = (TestPojo2) o;

			if (field2 != testPojo2.field2) return false;
			if (field3 != testPojo2.field3) return false;
			if (Float.compare(testPojo2.field4, field4) != 0) return false;
			if (field5 != testPojo2.field5) return false;
			if (Double.compare(testPojo2.field6, field6) != 0) return false;
			if (field1 != null ? !field1.equals(testPojo2.field1) : testPojo2.field1 != null) return false;
			return !(field7 != null ? !field7.equals(testPojo2.field7) : testPojo2.field7 != null);

		}

		@Override
		public int hashCode() {
			int result;
			long temp;
			result = field1 != null ? field1.hashCode() : 0;
			result = 31 * result + field2;
			result = 31 * result + (int) (field3 ^ (field3 >>> 32));
			result = 31 * result + (field4 != +0.0f ? Float.floatToIntBits(field4) : 0);
			result = 31 * result + field5;
			temp = Double.doubleToLongBits(field6);
			result = 31 * result + (int) (temp ^ (temp >>> 32));
			result = 31 * result + (field7 != null ? field7.hashCode() : 0);
			return result;
		}
	}

	public interface Test2 {
		int hash(TestPojo2 pojo);
	}

	public interface Test extends Comparator<TestPojo>, Comparable<Test> {
		Integer test(Integer argument);

		int hash(TestPojo pojo);

		int field1(TestPojo pojo);

		TestPojo setter(TestPojo pojo);

		TestPojo ctor();

		void setXY(int valueX, byte valueY);

		Integer getX();

		short getY();

		boolean allEqual(int var, int var1, int var2);

		boolean anyEqual(int var, int var1, int var2);

		void setPojoField1(TestPojo testPojo, int value);

		int getPojoField1(TestPojo testPojo);

		@Override
		int compare(TestPojo o1, TestPojo o2);

		@Override
		boolean equals(Object obj);

		@Override
		int compareTo(Test o);

		@Override
		String toString();

		void loop();
	}

	@org.junit.Test
	public void test() throws IllegalAccessException, InstantiationException {
		Expression local = let(constructor(TestPojo.class, value(1)));
		Class<Test> testClass = ClassBuilder.create(DefiningClassLoader.create(), Test.class)
				.withField("x", int.class)
				.withField("y", Long.class)
				.withMethod("compare", int.class, asList(TestPojo.class, TestPojo.class),
						compare(TestPojo.class, "field1", "field2"))
				.withMethod("int compareTo(io.datakernel.codegen.ExpressionTest$Test)",
						compareTo("x"))
				.withMethod("equals",
						asEquals("x"))
				.withMethod("setXY", sequence(
						set(self(), "x", arg(0)),
						set(self(), "y", arg(1))))
				.withMethod("test",
						add(arg(0), value(1L)))
				.withMethod("hash",
						hashCodeOfArgs(field(arg(0), "field1"), field(arg(0), "field2")))
				.withMethod("field1",
						field(arg(0), "field1"))
				.withMethod("setter", sequence(
						set(arg(0), "field1", value(10)),
						set(arg(0), "field2", value(20)),
						arg(0)))
				.withMethod("ctor", sequence(
						local,
						set(local, "field2", value(2)),
						local))
				.withMethod("getX",
						field(self(), "x"))
				.withMethod("getY",
						field(self(), "y"))
				.withMethod("allEqual",
						and(cmpEq(arg(0), arg(1)), cmpEq(arg(0), arg(2))))
				.withMethod("anyEqual",
						or(cmpEq(arg(0), arg(1)), cmpEq(arg(0), arg(2))))
				.withMethod("setPojoField1",
						call(arg(0), "setField1", arg(1)))
				.withMethod("getPojoField1",
						call(arg(0), "getField1"))
				.withMethod("toString",
						asString()
								.withQuotes("{", "}", ", ")
								.withArgument(field(self(), "x"))
								.withArgument("labelY: ", field(self(), "y")))
				.build();
		Test test = testClass.newInstance();

		assertEquals(11, (int) test.test(10));
		assertEquals(33, test.hash(new TestPojo(1, 2)));
		assertEquals(1, test.field1(new TestPojo(1, 2)));
		assertEquals(new TestPojo(10, 20), test.setter(new TestPojo(1, 2)));
		assertEquals(new TestPojo(1, 2), test.ctor());
		test.setXY(1, (byte) 10);
		assertEquals(1, (int) test.getX());
		assertEquals(10, test.getY());
		assertTrue(test.compare(new TestPojo(1, 10), new TestPojo(1, 10)) == 0);
		assertTrue(test.compare(new TestPojo(2, 10), new TestPojo(1, 10)) > 0);
		assertTrue(test.compare(new TestPojo(0, 10), new TestPojo(1, 10)) < 0);
		assertTrue(test.compare(new TestPojo(1, 0), new TestPojo(1, 10)) < 0);
		assertTrue(test.compareTo(test) == 0);

		Test test1 = testClass.newInstance();
		Test test2 = testClass.newInstance();

		test1.setXY(1, (byte) 10);
		test2.setXY(1, (byte) 10);
		assertTrue(test1.compareTo(test2) == 0);
		assertTrue(test1.equals(test2));
		test2.setXY(2, (byte) 10);
		assertTrue(test1.compareTo(test2) < 0);
		assertFalse(test1.equals(test2));
		test2.setXY(0, (byte) 10);
		assertTrue(test1.compareTo(test2) > 0);
		assertFalse(test1.equals(test2));

		assertTrue(test1.allEqual(1, 1, 1));
		assertFalse(test1.allEqual(1, 2, 1));
		assertFalse(test1.allEqual(1, 1, 2));
		assertFalse(test1.anyEqual(1, 2, 3));
		assertTrue(test1.anyEqual(1, 2, 1));
		assertTrue(test1.anyEqual(1, 1, 2));

		TestPojo testPojo = new TestPojo(1, 10);
		assertEquals(1, test1.getPojoField1(testPojo));
		test1.setPojoField1(testPojo, 2);
		assertEquals(2, test1.getPojoField1(testPojo));

		assertEquals("{1, labelY: 10}", test1.toString());
	}

	@org.junit.Test
	public void test2() throws IllegalAccessException, InstantiationException {
		Class<Test2> testClass = ClassBuilder.create(DefiningClassLoader.create(), Test2.class)
				.withMethod("hash",
						hashCodeOfArgs(field(arg(0), "field1"), field(arg(0), "field2"), field(arg(0), "field3"),
								field(arg(0), "field4"), field(arg(0), "field5"), field(arg(0), "field6"),
								field(arg(0), "field7"))).build();

		Test2 test = testClass.newInstance();
		TestPojo2 testPojo2 = new TestPojo2("randomString", 42, 666666, 43258.42342f, 54359878, 43252353278423.423468, "fhsduighrwqruqsd");

		assertEquals(testPojo2.hashCode(), test.hash(testPojo2));
	}

	@SuppressWarnings("unchecked")
	@org.junit.Test
	public void testComparator() {
		Comparator<TestPojo> comparator = ClassBuilder.create(DefiningClassLoader.create(), Comparator.class)
				.withMethod("compare",
						compare(TestPojo.class, "field1", "field2"))
				.buildClassAndCreateNewInstance();
		assertTrue(comparator.compare(new TestPojo(1, 10), new TestPojo(1, 10)) == 0);
	}

	public interface TestNeg {
		boolean negBoolean();

		int negByte();

		int negShort();

		int negChar();

		int negInt();

		long negLong();

		float negFloat();

		double negDouble();
	}

	@org.junit.Test
	public void testNeg() {
		boolean z = true;
		byte b = Byte.MAX_VALUE;
		short s = Short.MAX_VALUE;
		char c = Character.MAX_VALUE;
		int i = Integer.MAX_VALUE;
		long l = Long.MAX_VALUE;
		float f = Float.MAX_VALUE;
		double d = Double.MAX_VALUE;

		TestNeg testClass = ClassBuilder.create(DefiningClassLoader.create(), TestNeg.class)
				.withMethod("negBoolean", neg(value(z)))
				.withMethod("negShort", neg(value(s)))
				.withMethod("negByte", neg(value(b)))
				.withMethod("negChar", neg(value(c)))
				.withMethod("negInt", neg(value(i)))
				.withMethod("negLong", neg(value(l)))
				.withMethod("negFloat", neg(value(f)))
				.withMethod("negDouble", neg(value(d)))
				.buildClassAndCreateNewInstance();

		assertTrue(testClass.negBoolean() == !z);
		assertTrue(testClass.negShort() == -s);
		assertTrue(testClass.negByte() == -b);
		assertTrue(testClass.negChar() == -c);
		assertTrue(testClass.negInt() == -i);
		assertTrue(testClass.negLong() == -l);
		assertTrue(testClass.negFloat() == -f);
		assertTrue(testClass.negDouble() == -d);
	}

	public interface TestOperation {
		int remB();

		int remS();

		int remC();

		int remI();

		long remL();

		float remF();

		double remD();
	}

	@org.junit.Test
	public void testOperation() {
		byte b = Byte.MAX_VALUE;
		short s = Short.MAX_VALUE;
		char c = Character.MAX_VALUE;
		int i = Integer.MAX_VALUE;
		long l = Long.MAX_VALUE;
		float f = Float.MAX_VALUE;
		double d = Double.MAX_VALUE;

		TestOperation testClass = ClassBuilder.create(DefiningClassLoader.create(), TestOperation.class)
				.withMethod("remB", arithmeticOp(ExpressionArithmeticOp.Operation.REM, value(b), value(20)))
				.withMethod("remS", arithmeticOp(ExpressionArithmeticOp.Operation.REM, value(s), value(20)))
				.withMethod("remC", arithmeticOp(ExpressionArithmeticOp.Operation.REM, value(c), value(20)))
				.withMethod("remI", arithmeticOp(ExpressionArithmeticOp.Operation.REM, value(i), value(20)))
				.withMethod("remL", arithmeticOp(ExpressionArithmeticOp.Operation.REM, value(l), value(20)))
				.withMethod("remF", arithmeticOp(ExpressionArithmeticOp.Operation.REM, value(f), value(20)))
				.withMethod("remD", arithmeticOp(ExpressionArithmeticOp.Operation.REM, value(d), value(20)))
				.buildClassAndCreateNewInstance();

		assertTrue(testClass.remB() == (b % (20)));
		assertTrue(testClass.remS() == (s % (20)));
		assertTrue(testClass.remC() == (c % (20)));
		assertTrue(testClass.remI() == (i % (20)));
		assertTrue(testClass.remL() == (l % (20)));
		assertTrue(testClass.remF() == (f % (20)));
		assertTrue(testClass.remD() == (d % (20)));
	}

	public interface TestSH {
		int shlInt();

		long shlLong();

		int shrInt();

		long shrLong();

		int ushrInt();
	}

	@org.junit.Test
	public void testSH() {
		byte b = 8;
		int i = 2;
		long l = 4;

		TestSH testClass = ClassBuilder.create(DefiningClassLoader.create(), TestSH.class)
				.withMethod("shlInt", bitOp(ExpressionBitOp.Operation.SHL, value(b), value(i)))
				.withMethod("shlLong", bitOp(ExpressionBitOp.Operation.SHL, value(l), value(b)))
				.withMethod("shrInt", bitOp(ExpressionBitOp.Operation.SHR, value(b), value(i)))
				.withMethod("shrLong", bitOp(ExpressionBitOp.Operation.SHR, value(l), value(i)))
				.withMethod("ushrInt", bitOp(ExpressionBitOp.Operation.USHR, value(b), value(i)))
				.buildClassAndCreateNewInstance();

		assertTrue(testClass.shlInt() == (b << i));
		assertTrue(testClass.shlLong() == (l << b));
		assertTrue(testClass.shrInt() == (b >> i));
		assertTrue(testClass.shrLong() == (l >> i));
		assertTrue(testClass.ushrInt() == (b >>> i));
	}

	public interface TestBitMask {
		int andInt();

		int orInt();

		int xorInt();

		long andLong();

		long orLong();

		long xorLong();
	}

	@org.junit.Test
	public void testBitMask() {
		TestBitMask testClass = ClassBuilder.create(DefiningClassLoader.create(), TestBitMask.class)
				.withMethod("andInt", bitOp(ExpressionBitOp.Operation.AND, value(2), value(4)))
				.withMethod("orInt", bitOp(ExpressionBitOp.Operation.OR, value(2), value(4)))
				.withMethod("xorInt", bitOp(ExpressionBitOp.Operation.XOR, value(2), value(4)))
				.withMethod("andLong", bitOp(ExpressionBitOp.Operation.AND, value(2), value(4L)))
				.withMethod("orLong", bitOp(ExpressionBitOp.Operation.OR, value((byte) 2), value(4L)))
				.withMethod("xorLong", bitOp(ExpressionBitOp.Operation.XOR, value(2L), value(4L)))
				.buildClassAndCreateNewInstance();

		assertTrue(testClass.andInt() == (2 & 4));
		assertTrue(testClass.orInt() == (2 | 4));
		assertTrue(testClass.xorInt() == (2 ^ 4));
		assertTrue(testClass.andLong() == (2L & 4L));
		assertTrue(testClass.orLong() == (2L | 4L));
		assertTrue(testClass.xorLong() == (2L ^ 4L));
	}

	public interface TestCall {
		int callOther1(int i);

		long callOther2();

		int callStatic1(int i1, int i2);

		long callStatic2(long l);
	}

	@org.junit.Test
	public void testCall() {
		TestCall testClass = ClassBuilder.create(DefiningClassLoader.create(), TestCall.class)
				.withMethod("callOther1", call(self(), "method", arg(0)))
				.withMethod("callOther2", call(self(), "method"))
				.withMethod("method", int.class, asList(int.class), arg(0))
				.withMethod("method", long.class, Collections.<Class<?>>emptyList(), value(-1L))
				.withMethod("callStatic1", int.class, asList(int.class, int.class), callStaticSelf("method", arg(0), arg(1)))
				.withMethod("callStatic2", long.class, asList(long.class), callStaticSelf("method", arg(0)))
				.withStaticMethod("method", int.class, asList(int.class, int.class), arg(1))
				.withStaticMethod("method", long.class, asList(long.class), arg(0))
				.buildClassAndCreateNewInstance();

		assert (testClass.callOther1(100) == 100);
		assert (testClass.callOther2() == -1);
		assert (testClass.callStatic1(1, 2) == 2);
		assert (testClass.callStatic2(3L) == 3L);
	}

	public interface TestArgument {
		Object array(WriteFirstElement w, Object[] arr);

		Object write(WriteFirstElement w, Object o);
	}

	public static class WriteFirstElement {
		public Object writeFirst(Object[] i) {
			return i[0];
		}

		public Object write(Object o) {
			return o;
		}

	}

	@org.junit.Test
	public void testArgument() {
		TestArgument testClass = ClassBuilder.create(DefiningClassLoader.create(), TestArgument.class)
				.withMethod("array", call(arg(0), "writeFirst", arg(1)))
				.withMethod("write", call(arg(0), "write", arg(1)))
				.buildClassAndCreateNewInstance();

		assertTrue(testClass.array(new WriteFirstElement(), new Object[]{1000, 2, 3, 4}).equals(1000));
		assertTrue(testClass.write(new WriteFirstElement(), 1000).equals(1000));
	}

	public interface WriteAllListElement {
		void write(List listFrom, List listTo);

		void writeIter(Iterator iteratorFrom, List listTo);
	}

	@org.junit.Test
	public void testIterator() {
		List<Integer> listFrom = asList(1, 1, 2, 3, 5, 8);
		List<Integer> listTo1 = new ArrayList<>();
		List<Integer> listTo2 = new ArrayList<>();

		WriteAllListElement testClass = ClassBuilder.create(DefiningClassLoader.create(), WriteAllListElement.class)
				.withMethod("write", forEach(arg(0), new ForVar() {
					@Override
					public Expression forVar(Expression it) {
						return sequence(addListItem(arg(1), it), voidExp());
					}
				}))
				.withMethod("writeIter", forEach(arg(0), new ForVar() {
					@Override
					public Expression forVar(Expression it) {
						return sequence(addListItem(arg(1), it), voidExp());
					}
				}))
				.buildClassAndCreateNewInstance();

		testClass.write(listFrom, listTo1);
		testClass.writeIter(listFrom.iterator(), listTo2);

		assertEquals(listFrom.size(), listTo1.size());
		for (int i = 0; i < listFrom.size(); i++) {
			assertEquals(listFrom.get(i), (listTo1.get(i)));
		}

		assertEquals(listFrom.size(), listTo2.size());
		for (int i = 0; i < listFrom.size(); i++) {
			assertEquals(listFrom.get(i), (listTo2.get(i)));
		}
	}

	public interface WriteArrayElements {
		void write(Long[] a, List<Long> b);
	}

	@org.junit.Test
	public void testIteratorForArray() {
		Long[] intsFrom = new Long[]{1L, 1L, 2L, 3L, 5L, 8L};
		List<Long> list = new ArrayList<>();

		WriteArrayElements testClass = ClassBuilder.create(DefiningClassLoader.create(), WriteArrayElements.class)
				.withMethod("write", forEach(arg(0), new ForVar() {
					@Override
					public Expression forVar(Expression it) {
						return sequence(addListItem(arg(1), cast(it, Object.class)), voidExp());
					}
				}))
				.buildClassAndCreateNewInstance();

		testClass.write(intsFrom, list);
		for (int i = 0; i < intsFrom.length; i++) {
			assertEquals(intsFrom[i], list.get(i));
		}
	}

	public interface CastPrimitive {
		Object a();
	}

	@org.junit.Test
	public void testCastPrimitive() {
		CastPrimitive testClass = ClassBuilder.create(DefiningClassLoader.create(), CastPrimitive.class)
				.withMethod("a", value(1))
				.buildClassAndCreateNewInstance();

		assertEquals(testClass.a(), 1);
	}

	public interface Initializable {
		void init();
	}

	public interface Getter {
		Object get(Object obj);
	}

	@org.junit.Test
	public void testGetter() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Initializable intHolder = ClassBuilder.create(classLoader, Initializable.class)
				.withField("x", int.class)
				.withMethod("init", set(field(self(), "x"), value(42)))
				.buildClassAndCreateNewInstance();

		intHolder.init();

		Getter getter = ClassBuilder.create(classLoader, Getter.class)
				.withMethod("get", field(cast(arg(0), intHolder.getClass()), "x"))
				.buildClassAndCreateNewInstance();

		assertEquals(42, getter.get(intHolder));
	}

	@org.junit.Test
	public void testBuildedInstance() throws IllegalAccessException, InstantiationException {
		DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		Expression local = let(constructor(TestPojo.class, value(1)));
		Class<Test> testClass1 = ClassBuilder.create(definingClassLoader, Test.class)
				.withField("x", int.class)
				.withField("y", Long.class)
				.withMethod("compare", int.class, asList(TestPojo.class, TestPojo.class),
						compare(TestPojo.class, "field1", "field2"))
				.withMethod("int compareTo(io.datakernel.codegen.ExpressionTest$Test)",
						compareTo("x"))
				.withMethod("equals",
						asEquals("x"))
				.withMethod("setXY", sequence(
						set(field(self(), "x"), arg(0)),
						set(field(self(), "y"), arg(1))))
				.withMethod("test",
						add(arg(0), value(1L)))
				.withMethod("hash",
						hashCodeOfArgs(field(arg(0), "field1"), field(arg(0), "field2")))
				.withMethod("field1",
						field(arg(0), "field1"))
				.withMethod("setter", sequence(
						set(field(arg(0), "field1"), value(10)),
						set(field(arg(0), "field2"), value(20)),
						arg(0)))
				.withMethod("ctor", sequence(
						local,
						set(field(local, "field2"), value(2)),
						local))
				.withMethod("getX",
						field(self(), "x"))
				.withMethod("getY",
						field(self(), "y"))
				.withMethod("allEqual",
						and(cmpEq(arg(0), arg(1)), cmpEq(arg(0), arg(2))))
				.withMethod("anyEqual",
						or(cmpEq(arg(0), arg(1)), cmpEq(arg(0), arg(2))))
				.withMethod("setPojoField1",
						call(arg(0), "setField1", arg(1)))
				.withMethod("getPojoField1",
						call(arg(0), "getField1"))
				.withMethod("toString",
						asString()
								.withQuotes("{", "}", ", ")
								.withArgument(field(self(), "x"))
								.withArgument("labelY: ", field(self(), "y")))
				.build();

		Class<Test> testClass2 = ClassBuilder.create(definingClassLoader, Test.class)
				.withField("x", int.class)
				.withField("y", Long.class)
				.withMethod("compare", int.class, asList(TestPojo.class, TestPojo.class),
						compare(TestPojo.class, "field1", "field2"))
				.withMethod("int compareTo(io.datakernel.codegen.ExpressionTest$Test)",
						compareTo("x"))
				.withMethod("equals",
						asEquals("x"))
				.withMethod("setXY", sequence(
						set(field(self(), "x"), arg(0)),
						set(field(self(), "y"), arg(1))))
				.withMethod("test",
						add(arg(0), value(1L)))
				.withMethod("hash",
						hashCodeOfArgs(field(arg(0), "field1"), field(arg(0), "field2")))
				.withMethod("field1",
						field(arg(0), "field1"))
				.withMethod("setter", sequence(
						set(field(arg(0), "field1"), value(10)),
						set(field(arg(0), "field2"), value(20)),
						arg(0)))
				.withMethod("ctor", sequence(
						local,
						set(field(local, "field2"), value(2)),
						local))
				.withMethod("getX",
						field(self(), "x"))
				.withMethod("getY",
						field(self(), "y"))
				.withMethod("allEqual",
						and(cmpEq(arg(0), arg(1)), cmpEq(arg(0), arg(2))))
				.withMethod("anyEqual",
						or(cmpEq(arg(0), arg(1)), cmpEq(arg(0), arg(2))))
				.withMethod("setPojoField1",
						call(arg(0), "setField1", arg(1)))
				.withMethod("getPojoField1",
						call(arg(0), "getField1"))
				.withMethod("toString",
						asString()
								.withQuotes("{", "}", ", ")
								.withArgument(field(self(), "x"))
								.withArgument("labelY: ", field(self(), "y")))
				.build();

		assertEquals(testClass1, testClass2);
	}

	public interface TestCompare {
		boolean compareObjectLE(Integer i1, Integer i2);

		boolean comparePrimitiveLE(int i1, int i2);

		boolean compareObjectEQ(Integer i1, Integer i2);

		boolean compareObjectNE(Integer i1, Integer i2);

	}

	@org.junit.Test
	public void testCompare() throws IllegalAccessException, InstantiationException {
		DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		Class<TestCompare> test1 = ClassBuilder.create(definingClassLoader, TestCompare.class)
				.withMethod("compareObjectLE", cmp(PredicateDefCmp.Operation.LE, arg(0), arg(1)))
				.withMethod("comparePrimitiveLE", cmp(PredicateDefCmp.Operation.LE, arg(0), arg(1)))
				.withMethod("compareObjectEQ", cmp(PredicateDefCmp.Operation.EQ, arg(0), arg(1)))
				.withMethod("compareObjectNE", cmp(PredicateDefCmp.Operation.NE, arg(0), arg(1)))
				.build();

		TestCompare testCompare = test1.newInstance();
		assertTrue(testCompare.compareObjectLE(5, 5));
		assertTrue(testCompare.comparePrimitiveLE(5, 6));
		assertTrue(testCompare.compareObjectEQ(5, 5));
		assertTrue(testCompare.compareObjectNE(5, -5));
	}

	public static class StringHolder {
		public String string1;
		public String string2;

		public StringHolder(String string1, String string2) {
			this.string1 = string1;
			this.string2 = string2;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			StringHolder that = (StringHolder) o;
			return Objects.equals(string1, that.string1) &&
					Objects.equals(string2, that.string2);
		}

		@Override
		public int hashCode() {
			return Objects.hash(string1, string2);
		}

		@Override
		public String toString() {
			return "StringHolder{" +
					"string1='" + string1 + '\'' +
					", string2='" + string2 + '\'' +
					'}';
		}
	}

	public class StringHolderComparator implements Comparator<StringHolder> {
		public int compare(StringHolder var1, StringHolder var2) {
			String var1String1 = var1.string1;
			String var2String1 = var2.string1;
			int compare;
			if (var1String1 == null) {
				if (var2String1 != null) {
					return -1;
				}
			} else {
				if (var2String1 == null) {
					return 1;
				}

				compare = var1String1.compareTo(var2String1);
				if (compare != 0) {
					return compare;
				}
			}

			String var1String2 = var1.string2;
			String var2String2 = var2.string2;
			if (var1String2 == null) {
				if (var2String2 != null) {
					return -1;
				}
			} else {
				if (var2String2 == null) {
					return 1;
				}

				compare = var1String2.compareTo(var2String2);
				if (compare != 0) {
					return compare;
				}
			}

			compare = 0;
			return compare;
		}
	}

	@org.junit.Test
	public void testComparatorNullable() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Comparator generatedComparator = ClassBuilder.create(classLoader, Comparator.class)
				.withMethod("compare", ExpressionComparator.create()
						.with(leftField(StringHolder.class, "string1"), rightField(StringHolder.class, "string1"), true)
						.with(leftField(StringHolder.class, "string2"), rightField(StringHolder.class, "string2"), true))
				.buildClassAndCreateNewInstance();

		List<StringHolder> strings = Arrays.asList(new StringHolder(null, "b"), new StringHolder(null, "a"),
				new StringHolder("b", null), new StringHolder("c", "e"),
				new StringHolder("c", "d"), new StringHolder(null, null), new StringHolder("d", "z"),
				new StringHolder(null, null));
		List<StringHolder> strings2 = new ArrayList<>(strings);
		Collections.sort(strings, generatedComparator);
		Collections.sort(strings2, new StringHolderComparator());

		assertEquals(strings, strings2);
	}

	public interface TestInterface {
		double returnDouble();
	}

	public static abstract class TestAbstract implements TestInterface {
		protected abstract int returnInt();
	}

	@org.junit.Test
	public void testAbstractClassWithInterface() throws Exception {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		TestAbstract testObj = ClassBuilder.create(classLoader, TestAbstract.class)
				.withMethod("returnInt", value(42))
				.withMethod("returnDouble", value(-1.0))
				.buildClassAndCreateNewInstance();
		assertEquals(42, testObj.returnInt());
		assertEquals(-1.0, testObj.returnDouble(), 1E-5);
	}

	public static abstract class A {
		public int t() {
			return 40;
		}

		public abstract int a();
	}

	public interface B {
		Integer b();
	}

	public interface C {
		String c();
	}

	@org.junit.Test
	public void testMultipleInterfacesWithAbstract() {
		final DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		final A instance = ClassBuilder.create(definingClassLoader, A.class, asList(B.class, C.class))
				.withMethod("a", value(42))
				.withMethod("b", value(43))
				.withMethod("c", value("44"))
				.buildClassAndCreateNewInstance();

		assertEquals(instance.t(), 40);
		assertEquals(instance.a(), 42);
		assertEquals(((B) instance).b(), Integer.valueOf(43));
		assertEquals(((C) instance).c(), "44");
	}

	@org.junit.Test
	public void testMultipleInterfaces() {
		final DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		final B instance = ClassBuilder.create(definingClassLoader, B.class, Collections.<Class<?>>singletonList(C.class))
				.withMethod("b", value(43))
				.withMethod("c", value("44"))
				.buildClassAndCreateNewInstance();

		assertEquals(instance.b(), Integer.valueOf(43));
		assertEquals(((C) instance).c(), "44");

	}

	@org.junit.Test
	public void testNullableToString() {
		final DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		final B instance = ClassBuilder.create(definingClassLoader, B.class)
				.withMethod("b", nullRef(Integer.class))
				.withMethod("toString",
						asString()
								.withQuotes("{", "}", ", ")
								.withArgument(call(self(), "b")))
				.buildClassAndCreateNewInstance();

		assertEquals(instance.b(), null);
		assertEquals(instance.toString(), "{null}");
	}

	@org.junit.Test
	public void testSetSaveBytecode() throws IOException {
		final File folder = tempFolder.newFolder();
		final DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		final B instance = ClassBuilder.create(definingClassLoader, B.class)
				.withBytecodeSaveDir(folder.toPath())
				.withMethod("b", nullRef(Integer.class))
				.withMethod("toString",
						asString()
								.withQuotes("{", "}", ", ")
								.withArgument(call(self(), "b")))
				.buildClassAndCreateNewInstance();
		assertEquals(folder.list().length, 1);
		assertEquals(instance.b(), null);
		assertEquals(instance.toString(), "{null}");
	}

	public interface TestArraySet {
		Integer[] ints(Integer[] ints);
	}

	@org.junit.Test
	public void testArraySet() {
		final DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		final TestArraySet instance = ClassBuilder.create(definingClassLoader, TestArraySet.class)
				.withMethod("ints", sequence(setArrayItem(arg(0), value(0), cast(value(42), Integer.class)), arg(0)))
				.buildClassAndCreateNewInstance();
		Integer[] ints = new Integer[]{1, 2, 3, 4};

		assertArrayEquals(instance.ints(ints), new Integer[]{42, 2, 3, 4});
	}

	public interface TestCallStatic {
		int method(int a, int b);
	}

	@org.junit.Test
	public void testCallStatic() {
		final DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		final TestCallStatic instance = ClassBuilder.create(definingClassLoader, TestCallStatic.class)
				.withMethod("method", callStatic(Math.class, "min", arg(0), arg(1)))
				.buildClassAndCreateNewInstance();
		assertEquals(instance.method(5, 0), 0);
		assertEquals(instance.method(5, 10), 5);
	}

	public interface TestIsNull {
		boolean method(String a);
	}

	@org.junit.Test
	public void testIsNull() {
		final DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		final TestIsNull instance = ClassBuilder.create(definingClassLoader, TestIsNull.class)
				.withMethod("method", isNull(arg(0)))
				.buildClassAndCreateNewInstance();
		assertEquals(instance.method("42"), false);
		assertEquals(instance.method(null), true);
	}

	public interface TestNewArray {
		int[] ints(int size);

		String[] integers(int size);
	}

	@org.junit.Test
	public void testNewArray() {
		final DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		final TestNewArray instance = ClassBuilder.create(definingClassLoader, TestNewArray.class)
				.withMethod("ints", newArray(int[].class, arg(0)))
				.withMethod("integers", newArray(String[].class, arg(0)))
				.buildClassAndCreateNewInstance();
		assertEquals(instance.ints(1).length, 1);
		assertEquals(instance.integers(2).length, 2);
	}

	@org.junit.Test
	public void testStaticConstants() {
		final DefiningClassLoader definingClassLoader = DefiningClassLoader.create();
		Object testObject = new Object();
		final Getter instance = ClassBuilder.create(definingClassLoader, Getter.class)
				.withMethod("get", value(testObject))
				.buildClassAndCreateNewInstance();
		assertTrue(testObject == instance.get(null));
	}
}