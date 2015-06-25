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

import org.objectweb.asm.Type;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.objectweb.asm.Type.getType;

/**
 * Defines list of possibilities for creating the dynamic object
 */
public final class FunctionDefs {
	private FunctionDefs() {
	}

	public static FunctionDefSequence sequence(List<FunctionDef> parts) {
		return new FunctionDefSequence(parts);
	}

	/**
	 * Returns sequence of operations which will be processed one after the other
	 *
	 * @param parts list of operations
	 * @return new instance of the FunctionDefSequence
	 */
	public static FunctionDefSequence sequence(FunctionDef... parts) {
		return new FunctionDefSequence(asList(parts));
	}

	/**
	 * Return new variable which will process function
	 *
	 * @param name        name of variable
	 * @param functionDef function which will be processed when variable will be used
	 * @return new instance of the FunctionDef
	 */
	public static FunctionDef let(String name, FunctionDef functionDef) {
		return new FunctionDefLet(functionDef, name);
	}

	/**
	 * Return variable which was created with method 'let' before
	 *
	 * @param name name of variable
	 * @return new instance of the FunctionDef
	 */
	public static FunctionDef var(String name) {
		return new FunctionDefVar(name);
	}

	/**
	 * Sets the value from argument 'from' in argument 'to'
	 *
	 * @param to   variable which will be changed
	 * @param from variable which will change
	 * @return new instance of the FunctionDef
	 */
	public static FunctionDef set(StoreDef to, FunctionDef from) {
		return new FunctionDefSet(to, from);
	}

	/**
	 * Adds function to the context cache
	 *
	 * @param functionDef function which will be cached
	 * @return new instance of the FunctionDef
	 */
	public static FunctionDef cache(FunctionDef functionDef) {
		return new FunctionDefCache(functionDef);
	}

	/**
	 * Casts functionDef to the type
	 *
	 * @param functionDef functions which will be casted
	 * @param type        function will be casted to the 'type'
	 * @return new instance of the FunctionDef which is casted to the type
	 */
	public static FunctionDef cast(FunctionDef functionDef, Type type) {
		return new FunctionDefCast(functionDef, type);
	}

	/**
	 * Casts functionDef to the type
	 *
	 * @param functionDef functions which will be casted
	 * @param type        function will be casted to the 'type'
	 * @return new instance of the FunctionDef which is casted to the type
	 */
	public static FunctionDef cast(FunctionDef functionDef, Class<?> type) {
		return cast(functionDef, getType(type));
	}

	/**
	 * Returns the field from owner
	 *
	 * @param owner owner of the field
	 * @param field name of the field which will be returned
	 * @return new instance of the VarField
	 */
	public static VarField field(FunctionDef owner, String field) {
		return new VarField(owner, field);
	}

	/**
	 * Returns current instance
	 *
	 * @return current instance of the FunctionDef
	 */
	public static FunctionDef self() {
		return new VarThis();
	}

	/**
	 * Returns value which ordinal number is 'argument'
	 *
	 * @param argument ordinal number in list of arguments
	 * @return new instance of the VarArg
	 */
	public static FunctionDef arg(int argument) {
		return new VarArg(argument);
	}

	/**
	 * Compares arguments
	 *
	 * @param eq    operation which will be used for the arguments
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return new instance of the PredicateDefCmp
	 */
	public static PredicateDefCmp cmp(PredicateDefCmp.Operation eq, FunctionDef left, FunctionDef right) {
		return new PredicateDefCmp(eq, left, right);
	}

	/**
	 * Verifies that the arguments are equal
	 *
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return new instance of the PredicateDefCmp
	 */
	public static PredicateDefCmp cmpEq(FunctionDef left, FunctionDef right) {
		return cmp(PredicateDefCmp.Operation.EQ, left, right);
	}

	public static PredicateDefCmp cmpGe(FunctionDef left, FunctionDef right) {
		return cmp(PredicateDefCmp.Operation.GE, left, right);
	}

	public static PredicateDefCmp cmpLe(FunctionDef left, FunctionDef right) {
		return cmp(PredicateDefCmp.Operation.LE, left, right);
	}

	/**
	 * Returns result of logical 'and' for the list of predicates
	 *
	 * @param predicateDefs list of the predicate
	 * @return new instance of the PredicateDefAnd
	 */
	public static PredicateDefAnd and(List<PredicateDef> predicateDefs) {
		return new PredicateDefAnd(predicateDefs);
	}

	/**
	 * Returns result of logical 'and' for the list of predicates
	 *
	 * @param predicateDefs list of the predicate
	 * @return new instance of the PredicateDefAnd
	 */
	public static PredicateDefAnd and(PredicateDef... predicateDefs) {
		return and(asList(predicateDefs));
	}

	/**
	 * Returns result of logical 'or' for the list of predicates
	 *
	 * @param predicateDefs list of the predicate
	 * @return new instance of the PredicateDefOr
	 */
	public static PredicateDefOr or(List<PredicateDef> predicateDefs) {
		return new PredicateDefOr(predicateDefs);
	}

	/**
	 * Returns result of logical 'or' for the list of predicates
	 *
	 * @param predicateDefs list of the predicate
	 * @return new instance of the PredicateDefAnd
	 */
	public static PredicateDefOr or(PredicateDef... predicateDefs) {
		return or(asList(predicateDefs));
	}

	/**
	 * Verifies that the fields are equal
	 *
	 * @param fields list of the fields
	 * @return new instance of the FunctionDef
	 */
	public static FunctionDef asEquals(List<String> fields) {
		PredicateDefAnd predicate = and();
		for (String field : fields) {
			predicate.add(cmpEq(
					field(self(), field),
					field(cast(arg(0), FunctionDefCast.THIS_TYPE), field)));
		}
		return predicate;
	}

	/**
	 * Returns the string which was constructed from fields
	 *
	 * @param fields list of the fields
	 * @return new instance of the FunctionDefToString
	 */
	public static FunctionDefToString asString(List<String> fields) {
		FunctionDefToString toString = new FunctionDefToString();
		for (String field : fields) {
			toString.add(field + "=", field(self(), field));
		}
		return toString;
	}

	/**
	 * Returns the string which was constructed from fields
	 *
	 * @param fields list of the fields
	 * @return new instance of the FunctionDefToString
	 */
	public static FunctionDefToString asString(String... fields) {
		return asString(asList(fields));
	}

	/**
	 * Verifies that the fields are equal
	 *
	 * @param fields list of the fields
	 * @return new instance of the FunctionDef
	 */
	public static FunctionDef asEquals(String... fields) {
		return asEquals(asList(fields));
	}

	/**
	 * Returns new instance of the FunctionDefComparator
	 *
	 * @return new instance of the FunctionDefComparator
	 */
	public static FunctionDefComparator comparator() {
		return new FunctionDefComparator();
	}

	/**
	 * Compares the fields
	 *
	 * @param type   type of the fields
	 * @param fields fields which will be compared
	 * @return new instance of the FunctionDefComparator
	 */
	public static FunctionDefComparator compare(Class<?> type, List<String> fields) {
		FunctionDefComparator comparator = comparator();
		for (String field : fields) {
			comparator.add(
					field(cast(arg(0), type), field),
					field(cast(arg(1), type), field));
		}
		return comparator;
	}

	/**
	 * Compares the fields
	 *
	 * @param type   type of the fields
	 * @param fields fields which will be compared
	 * @return new instance of the FunctionDefComparator
	 */
	public static FunctionDefComparator compare(Class<?> type, String... fields) {
		return compare(type, asList(fields));
	}

	/**
	 * Compares the fields
	 *
	 * @param fields list of the fields with will be compared
	 * @return new instance of the FunctionDefComparator
	 */
	public static FunctionDefComparator compareTo(List<String> fields) {
		FunctionDefComparator comparator = comparator();
		for (String field : fields) {
			comparator.add(
					field(self(), field),
					field(cast(arg(0), FunctionDefCast.THIS_TYPE), field));
		}
		return comparator;
	}

	/**
	 * Compares the fields
	 *
	 * @param fields list of the fields with will be compared
	 * @return new instance of the FunctionDefComparator
	 */
	public static FunctionDefComparator compareTo(String... fields) {
		return compareTo(asList(fields));
	}

	/**
	 * Returns new constant for the value
	 *
	 * @param value value which will be created as constant
	 * @return new instance of the FunctionDefConstant
	 */
	public static FunctionDefConstant value(Object value) {
		return new FunctionDefConstant(value);
	}

	/**
	 * Returns hash of the fields
	 *
	 * @param fields list of the fields which will be hashed
	 * @return new instance of the FunctionDefHash
	 */
	public static FunctionDefHash hashCodeOfThis(List<String> fields) {
		List<FunctionDef> arguments = new ArrayList<>();
		for (String field : fields) {
			arguments.add(field(new VarThis(), field));
		}
		return new FunctionDefHash(arguments);
	}

	public static FunctionDefHash hashCodeOfThis(String... fields) {
		return hashCodeOfThis(asList(fields));
	}

	/**
	 * Returns a hash code which calculated from fields
	 *
	 * @param fields list of the fields which will be hashed
	 * @return new instance of the FunctionDefHash
	 */
	public static FunctionDefHash hashCodeOfArgs(List<FunctionDef> fields) {
		return new FunctionDefHash(fields);
	}

	/**
	 * Returns a hash code which calculated from fields
	 *
	 * @param fields list of the fields which will be hashed
	 * @return new instance of the FunctionDefHash
	 */
	public static FunctionDefHash hashCodeOfArgs(FunctionDef... fields) {
		return hashCodeOfArgs(asList(fields));
	}

	public static FunctionDefArithmeticOp arithmeticOp(FunctionDefArithmeticOp.Operation op, FunctionDef left, FunctionDef right) {
		return new FunctionDefArithmeticOp(op, left, right);
	}

	/**
	 * Returns sum of arguments
	 *
	 * @param left  first argument whick will be added
	 * @param right second argument which will be added
	 * @return new instance of the FunctionDefArithmeticOp
	 */
	public static FunctionDefArithmeticOp add(FunctionDef left, FunctionDef right) {
		return new FunctionDefArithmeticOp(FunctionDefArithmeticOp.Operation.ADD, left, right);
	}

	/**
	 * Returns new instance of class
	 *
	 * @param type   type of the constructor
	 * @param fields fields for constructor
	 * @return new instance of the FunctionDefConstructor
	 */
	public static FunctionDefConstructor constructor(Class<?> type, FunctionDef... fields) {
		return new FunctionDefConstructor(type, fields);
	}

	/**
	 * Calls method which defines static in the class
	 *
	 * @param owner      owner of the method
	 * @param methodName name of the method in the class
	 * @param arguments  list of the arguments for the method
	 * @return new instance of the FunctionDefCall
	 */
	public static FunctionDefCall call(FunctionDef owner, String methodName, FunctionDef... arguments) {
		return new FunctionDefCall(owner, methodName, arguments);
	}

}
