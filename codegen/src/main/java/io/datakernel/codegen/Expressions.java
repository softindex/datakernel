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
public final class Expressions {
	private Expressions() {
	}

	public static ExpressionSequence sequence(List<Expression> parts) {
		return new ExpressionSequence(parts);
	}

	/**
	 * Returns sequence of operations which will be processed one after the other
	 *
	 * @param parts list of operations
	 * @return new instance of the ExpressionSequence
	 */
	public static ExpressionSequence sequence(Expression... parts) {
		return new ExpressionSequence(asList(parts));
	}

	/**
	 * Return new variable which will process expression
	 *
	 * @param expression expression which will be processed when variable will be used
	 * @return new instance of the Expression
	 */
	public static ExpressionLet let(Expression expression) {
		return new ExpressionLet(expression);
	}

	/**
	 * Sets the value from argument 'from' in argument 'to'
	 *
	 * @param to   variable which will be changed
	 * @param from variable which will change
	 * @return new instance of the Expression
	 */
	public static Expression set(StoreDef to, Expression from) {
		return new ExpressionSet(to, from);
	}

	/**
	 * Casts expression to the type
	 *
	 * @param expression expressions which will be casted
	 * @param type       expression will be casted to the 'type'
	 * @return new instance of the Expression which is casted to the type
	 */
	public static Expression cast(Expression expression, Type type) {
		return new ExpressionCast(expression, type);
	}

	/**
	 * Casts expression to the type
	 *
	 * @param expression expressions which will be casted
	 * @param type       expression will be casted to the 'type'
	 * @return new instance of the Expression which is casted to the type
	 */
	public static Expression cast(Expression expression, Class<?> type) {
		return cast(expression, getType(type));
	}

	/**
	 * Returns the field from owner
	 *
	 * @param owner owner of the field
	 * @param field name of the field which will be returned
	 * @return new instance of the VarField
	 */
	public static VarField getter(Expression owner, String field) {
		return new VarField(owner, field);
	}

	/**
	 * Sets value to the field in owner
	 *
	 * @param owner owner of the field
	 * @param field name of field which will be changed
	 * @param value new value for the field
	 * @return new instance of the ExpressionSet
	 */
	public static Expression setter(Expression owner, String field, Expression value) {
		return set(getter(owner, field), value);
	}

	/**
	 * Returns current instance
	 *
	 * @return current instance of the Expression
	 */
	public static Expression self() {
		return new VarThis();
	}

	/**
	 * Returns value which ordinal number is 'argument'
	 *
	 * @param argument ordinal number in list of arguments
	 * @return new instance of the VarArg
	 */
	public static VarArg arg(int argument) {
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
	public static PredicateDefCmp cmp(PredicateDefCmp.Operation eq, Expression left, Expression right) {
		return new PredicateDefCmp(eq, left, right);
	}

	/**
	 * Verifies that the arguments are equal
	 *
	 * @param left  first argument which will be compared
	 * @param right second argument which will be compared
	 * @return new instance of the PredicateDefCmp
	 */
	public static PredicateDefCmp cmpEq(Expression left, Expression right) {
		return cmp(PredicateDefCmp.Operation.EQ, left, right);
	}

	public static PredicateDefCmp cmpGe(Expression left, Expression right) {
		return cmp(PredicateDefCmp.Operation.GE, left, right);
	}

	public static PredicateDefCmp cmpLe(Expression left, Expression right) {
		return cmp(PredicateDefCmp.Operation.LE, left, right);
	}

	public static PredicateDefCmp cmpLt(Expression left, Expression right) {
		return cmp(PredicateDefCmp.Operation.LT, left, right);
	}

	public static PredicateDefCmp cmpNe(Expression left, Expression right) {
		return cmp(PredicateDefCmp.Operation.NE, left, right);
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
	 * @return new instance of the Expression
	 */
	public static Expression asEquals(List<String> fields) {
		PredicateDefAnd predicate = and();
		for (String field : fields) {
			predicate.add(cmpEq(
					getter(self(), field),
					getter(cast(arg(0), ExpressionCast.THIS_TYPE), field)));
		}
		return predicate;
	}

	/**
	 * Returns the string which was constructed from fields
	 *
	 * @param fields list of the fields
	 * @return new instance of the ExpressionToString
	 */
	public static ExpressionToString asString(List<String> fields) {
		ExpressionToString toString = new ExpressionToString();
		for (String field : fields) {
			toString.add(field + "=", getter(self(), field));
		}
		return toString;
	}

	/**
	 * Returns the string which was constructed from fields
	 *
	 * @param fields list of the fields
	 * @return new instance of the ExpressionToString
	 */
	public static ExpressionToString asString(String... fields) {
		return asString(asList(fields));
	}

	/**
	 * Verifies that the fields are equal
	 *
	 * @param fields list of the fields
	 * @return new instance of the Expression
	 */
	public static Expression asEquals(String... fields) {
		return asEquals(asList(fields));
	}

	/**
	 * Returns new instance of the ExpressionComparator
	 *
	 * @return new instance of the ExpressionComparator
	 */
	public static ExpressionComparator comparator() {
		return new ExpressionComparator();
	}

	public static ExpressionComparatorNullable comparatorNullable() {
		return new ExpressionComparatorNullable();
	}

	/**
	 * Compares the fields
	 *
	 * @param type   type of the fields
	 * @param fields fields which will be compared
	 * @return new instance of the ExpressionComparator
	 */
	public static ExpressionComparator compare(Class<?> type, List<String> fields) {
		ExpressionComparator comparator = comparator();
		for (String field : fields) {
			comparator.add(
					getter(cast(arg(0), type), field),
					getter(cast(arg(1), type), field));
		}
		return comparator;
	}

	/**
	 * Compares the fields
	 *
	 * @param type   type of the fields
	 * @param fields fields which will be compared
	 * @return new instance of the ExpressionComparator
	 */
	public static ExpressionComparator compare(Class<?> type, String... fields) {
		return compare(type, asList(fields));
	}

	/**
	 * Compares the fields
	 *
	 * @param fields list of the fields with will be compared
	 * @return new instance of the ExpressionComparator
	 */
	public static ExpressionComparator compareTo(List<String> fields) {
		ExpressionComparator comparator = comparator();
		for (String field : fields) {
			comparator.add(
					getter(self(), field),
					getter(cast(arg(0), ExpressionCast.THIS_TYPE), field));
		}
		return comparator;
	}

	/**
	 * Compares the fields
	 *
	 * @param fields list of the fields with will be compared
	 * @return new instance of the ExpressionComparator
	 */
	public static ExpressionComparator compareTo(String... fields) {
		return compareTo(asList(fields));
	}

	/**
	 * Returns new constant for the value
	 *
	 * @param value value which will be created as constant
	 * @return new instance of the ExpressionConstant
	 */
	public static ExpressionConstant value(Object value) {
		return new ExpressionConstant(value);
	}

	/**
	 * Returns hash of the fields
	 *
	 * @param fields list of the fields which will be hashed
	 * @return new instance of the ExpressionHash
	 */
	public static ExpressionHash hashCodeOfThis(List<String> fields) {
		List<Expression> arguments = new ArrayList<>();
		for (String field : fields) {
			arguments.add(getter(new VarThis(), field));
		}
		return new ExpressionHash(arguments);
	}

	public static ExpressionHash hashCodeOfThis(String... fields) {
		return hashCodeOfThis(asList(fields));
	}

	/**
	 * Returns a hash code which calculated from fields
	 *
	 * @param fields list of the fields which will be hashed
	 * @return new instance of the ExpressionHash
	 */
	public static ExpressionHash hashCodeOfArgs(List<Expression> fields) {
		return new ExpressionHash(fields);
	}

	/**
	 * Returns a hash code which calculated from fields
	 *
	 * @param fields list of the fields which will be hashed
	 * @return new instance of the ExpressionHash
	 */
	public static ExpressionHash hashCodeOfArgs(Expression... fields) {
		return hashCodeOfArgs(asList(fields));
	}

	public static ExpressionArithmeticOp arithmeticOp(ExpressionArithmeticOp.Operation op, Expression left, Expression right) {
		return new ExpressionArithmeticOp(op, left, right);
	}

	/**
	 * Returns sum of arguments
	 *
	 * @param left  first argument whick will be added
	 * @param right second argument which will be added
	 * @return new instance of the ExpressionArithmeticOp
	 */
	public static ExpressionArithmeticOp add(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ExpressionArithmeticOp.Operation.ADD, left, right);
	}

	public static ExpressionArithmeticOp sub(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ExpressionArithmeticOp.Operation.SUB, left, right);
	}

	public static ExpressionArithmeticOp mul(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ExpressionArithmeticOp.Operation.MUL, left, right);
	}

	public static ExpressionArithmeticOp div(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ExpressionArithmeticOp.Operation.DIV, left, right);
	}

	public static ExpressionArithmeticOp rem(Expression left, Expression right) {
		return new ExpressionArithmeticOp(ExpressionArithmeticOp.Operation.REM, left, right);
	}

	/**
	 * Returns new instance of class
	 *
	 * @param type   type of the constructor
	 * @param fields fields for constructor
	 * @return new instance of the ExpressionConstructor
	 */
	public static ExpressionConstructor constructor(Class<?> type, Expression... fields) {
		return new ExpressionConstructor(type, fields);
	}

	/**
	 * Calls method which defines static in the class
	 *
	 * @param owner      owner of the method
	 * @param methodName name of the method in the class
	 * @param arguments  list of the arguments for the method
	 * @return new instance of the ExpressionCall
	 */
	public static ExpressionCall call(Expression owner, String methodName, Expression... arguments) {
		return new ExpressionCall(owner, methodName, arguments);
	}

	public static ExpressionIf choice(PredicateDef condition, Expression left, Expression right) {
		return new ExpressionIf(condition, left, right);
	}

	public static ExpressionIf ifTrue(PredicateDefCmp condition, Expression ifTrue) {
		return choice(condition, ifTrue, null);
	}

	public static ExpressionLength length(Expression field) {
		return new ExpressionLength(field);
	}

	public static ExpressionNewArray newArray(Class<?> type, Expression length) {
		return new ExpressionNewArray(type, length);
	}

	public static ExpressionCallStatic callStatic(Class<?> owner, String method, Expression... arguments) {
		return new ExpressionCallStatic(owner, method, arguments);
	}

	public static ExpressionCallStaticSelf callStaticSelf(String method, Expression... arguments) {
		return new ExpressionCallStaticSelf(method, arguments);
	}

	public static ExpressionArrayGet get(Expression array, Expression nom) {
		return new ExpressionArrayGet(array, nom);
	}

	public static ExpressionCmpNull ifNull(Expression field) {
		return new ExpressionCmpNull(field);
	}

	public static ExpressionCmpNotNull ifNotNull(Expression field) {
		return new ExpressionCmpNotNull(field);
	}

	public static ExpressionNull nullRef(Class<?> type) {
		return new ExpressionNull(type);
	}

	public static ExpressionNull nullRef(Type type) {
		return new ExpressionNull(type);
	}

	public static ExpressionVoid voidExp() {
		return ExpressionVoid.instance;
	}

	public static ExpressionSwitch switchForPosition(Expression position, List<Expression> list) {
		return new ExpressionSwitch(position, list);
	}

	public static ExpressionSwitch switchForPosition(Expression position, Expression... expressions) {
		return new ExpressionSwitch(position, asList(expressions));
	}

	public static ExpressionSwitchForKey switchForKey(Expression key, List<Expression> listKey, List<Expression> listValue) {
		return new ExpressionSwitchForKey(key, listKey, listValue);
	}

	public static ExpressionArraySet setArrayItem(Expression array, Expression position, Expression newElement) {
		return new ExpressionArraySet(array, position, newElement);
	}

	public static ExpressionFor expressionFor(Expression start, Expression length, ForVar forVar) {
		return new ExpressionFor(start, length, forVar);
	}

	public static ExpressionFor expressionFor(Expression length, ForVar forVar) {
		return new ExpressionFor(length, forVar);
	}

	public static ExpressionMapForEach mapForEach(Expression field, ForVar forKey, ForVar forValue) {
		return new ExpressionMapForEach(field, forKey, forValue);
	}

	public static ForEachHppcMap hppcMapForEach(Class<?> iteratorType, Expression value, ForVar forKey, ForVar forValue) {
		return new ForEachHppcMap(iteratorType, value, forKey, forValue);
	}

	public static ForEachHppcSet hppcSetForEach(Class<?> iteratorType, Expression field, ForVar forVar) {
		return new ForEachHppcSet(iteratorType, field, forVar);
	}

	public static ExpressionNeg neg(Expression arg) {
		return new ExpressionNeg(arg);
	}

	public static ExpressionBitOp bitOp(ExpressionBitOp.Operation op, Expression value, Expression shift) {
		return new ExpressionBitOp(op, value, shift);
	}

	public static Expression setListItem(Expression list, Expression position, Expression value) {
		return call(list, "set", position, value);
	}

	public static Expression getListItem(Expression list, Expression position) {
		return call(list, "get", position);
	}

	public static Expression addListItem(Expression list, Expression value) {
		return call(list, "add", value);
	}

	public static ExpressionIteratorForEach forEach(Expression collection, ForVar forCollection) {
		return new ExpressionIteratorForEach(collection, forCollection);
	}

	public static ExpressionIteratorForEach forEach(Expression collection, Class<?> type, ForVar forCollection) {
		return new ExpressionIteratorForEach(collection, type, forCollection);
	}
}
