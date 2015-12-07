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

package io.datakernel.examples;

import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.utils.DefiningClassLoader;

import static io.datakernel.codegen.Expressions.*;

/**
 * In this example a Class that implements the specified interface is dynamically created.
 * Methods are constructed programmatically using our fluent API built on top of ASM.
 */
public class DynamicClassCreationExample {
	public static void main(String[] args) throws IllegalAccessException, InstantiationException {
		// Construct a Class that implements Test interface
		Class<Test> testClass = new AsmBuilder<>(new DefiningClassLoader(), Test.class)
				// declare fields
				.field("x", int.class)
				.field("y", String.class)
						// setter for both fields - a sequence of actions
				.method("setXY", sequence(
						setter(self(), "x", arg(0)),
						setter(self(), "y", arg(1))))
				.method("getX", getter(self(), "x"))
				.method("getY", getter(self(), "y"))
						// compareTo, equals, hashCode and toString methods implementations follow the standard convention
				.method("int compareTo(Test)", compareTo("x", "y"))
				.method("equals", asEquals("x", "y"))
				.method("hashOfPojo", hashCodeOfArgs(getter(arg(0), "field1"), getter(arg(0), "field2")))
				.method("hash", hashCodeOfArgs(getter(self(), "x"), getter(self(), "y")))
				.method("toString", asString()
						.quotes("{", "}", ", ")
						.add("field1: ", getter(self(), "x"))
						.add("field2: ", getter(self(), "y")))
				.defineClass();

		// Instantiate two objects of dynamically defined class
		Test test1 = testClass.newInstance();
		Test test2 = testClass.newInstance();

		test1.setXY(5, "First");
		test2.setXY(test1.getX() * 2, "Second");

		System.out.println("test1 = " + test1);
		System.out.println("test2 = " + test2);

		System.out.println("test1.equals(test2)     = " + test1.equals(test2));

		// Compare dynamically created hashing implementation with the conventional one
		TestPojo testPojo = new TestPojo(5, "First");
		System.out.println(testPojo);
		System.out.println("test1.hash(testPojo)    = " + test1.hashOfPojo(testPojo));
		System.out.println("test1.hash()            = " + test1.hash());
		System.out.println("testPojo.hashCode()     = " + testPojo.hashCode());
	}

	public interface Test extends Comparable<Test> {
		void setXY(int valueX, String valueY);

		int getX();

		String getY();

		int hashOfPojo(TestPojo testPojo);

		int hash();

		@Override
		int compareTo(Test o);

		@Override
		String toString();

		@Override
		boolean equals(Object obj);
	}

	public static class TestPojo {
		public int field1;
		public String field2;

		public TestPojo(int field1, String field2) {
			this.field1 = field1;
			this.field2 = field2;
		}

		@Override
		public int hashCode() {
			int result = field1;
			result = 31 * result + field2.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "TestPojo{field1=" + field1 + ", field2=" + field2 + '}';
		}
	}
}




