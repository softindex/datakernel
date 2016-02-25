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

package io.datakernel.cube.api;

import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Expressions;
import io.datakernel.util.Function;

import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.codegen.Expressions.*;

public final class ReportingDSL {
	private ReportingDSL() {}

	/* Addition */
	public static ReportingDSLExpression add(String measure1, String measure2) {
		Expression expression = Expressions.add(cast(getter(self(), measure1), double.class), getter(self(), measure2));
		Set<String> measureDependencies = newHashSet(measure1, measure2);
		return new ReportingDSLExpression(expression, measureDependencies);
	}

	public static ReportingDSLExpression add(ReportingDSLExpression reportingExpression, String measure) {
		Expression expression = Expressions.add(reportingExpression.getExpression(), cast(getter(self(), measure), double.class));
		return new ReportingDSLExpression(expression, mergeMeasureDependencies(reportingExpression, measure));
	}

	public static ReportingDSLExpression add(ReportingDSLExpression reportingExpression, double value) {
		Expression expression = Expressions.add(reportingExpression.getExpression(), value(value));
		return new ReportingDSLExpression(expression, reportingExpression.getMeasureDependencies());
	}

	public static ReportingDSLExpression add(ReportingDSLExpression reportingExpression1, ReportingDSLExpression reportingExpression2) {
		Expression expression = Expressions.add(reportingExpression1.getExpression(), reportingExpression2.getExpression());
		return new ReportingDSLExpression(expression, mergeMeasureDependencies(reportingExpression1, reportingExpression2));
	}

	/* Subtraction */
	public static ReportingDSLExpression subtract(ReportingDSLExpression reportingExpression1, ReportingDSLExpression reportingExpression2) {
		Expression expression = sub(reportingExpression1.getExpression(), reportingExpression2.getExpression());
		return new ReportingDSLExpression(expression, mergeMeasureDependencies(reportingExpression1, reportingExpression2));
	}

	public static ReportingDSLExpression subtract(String measure1, String measure2) {
		Expression expression = sub(cast(getter(self(), measure1), double.class), getter(self(), measure2));
		Set<String> measureDependencies = newHashSet(measure1, measure2);
		return new ReportingDSLExpression(expression, measureDependencies);
	}

	public static ReportingDSLExpression subtract(ReportingDSLExpression reportingExpression, String measure) {
		Expression expression = sub(reportingExpression.getExpression(), cast(getter(self(), measure), double.class));
		return new ReportingDSLExpression(expression, mergeMeasureDependencies(reportingExpression, measure));
	}

	public static ReportingDSLExpression subtract(String measure, ReportingDSLExpression reportingExpression) {
		Expression expression = sub(cast(getter(self(), measure), double.class), reportingExpression.getExpression());
		return new ReportingDSLExpression(expression, mergeMeasureDependencies(reportingExpression, measure));
	}

	public static ReportingDSLExpression subtract(ReportingDSLExpression reportingExpression, double value) {
		Expression expression = sub(reportingExpression.getExpression(), value(value));
		return new ReportingDSLExpression(expression, reportingExpression.getMeasureDependencies());
	}

	public static ReportingDSLExpression subtract(double value, ReportingDSLExpression reportingExpression) {
		Expression expression = sub(value(value), reportingExpression.getExpression());
		return new ReportingDSLExpression(expression, reportingExpression.getMeasureDependencies());
	}

	/* Division */
	public static ReportingDSLExpression divide(String measure1, String measure2) {
		Expression expression = getDivisionExpression(cast(getter(self(), measure1), double.class), cast(getter(self(), measure2), double.class));
		Set<String> measureDependencies = newHashSet(measure1, measure2);
		return new ReportingDSLExpression(expression, measureDependencies);
	}

	public static ReportingDSLExpression divide(ReportingDSLExpression reportingExpression, String measure) {
		Expression expression = getDivisionExpression(reportingExpression.getExpression(), cast(getter(self(), measure), double.class));
		return new ReportingDSLExpression(expression, mergeMeasureDependencies(reportingExpression, measure));
	}

	public static ReportingDSLExpression divide(String measure, ReportingDSLExpression reportingExpression) {
		Expression expression = getDivisionExpression(cast(getter(self(), measure), double.class), reportingExpression.getExpression());
		return new ReportingDSLExpression(expression, mergeMeasureDependencies(reportingExpression, measure));
	}

	public static ReportingDSLExpression divide(ReportingDSLExpression reportingExpression, double value) {
		Expression expression = getDivisionExpression(reportingExpression.getExpression(), value(value));
		return new ReportingDSLExpression(expression, reportingExpression.getMeasureDependencies());
	}

	public static ReportingDSLExpression divide(double value, ReportingDSLExpression reportingExpression) {
		Expression expression = getDivisionExpression(value(value), reportingExpression.getExpression());
		return new ReportingDSLExpression(expression, reportingExpression.getMeasureDependencies());
	}

	public static ReportingDSLExpression divide(ReportingDSLExpression reportingExpression1, ReportingDSLExpression reportingExpression2) {
		Expression expression = getDivisionExpression(reportingExpression1.getExpression(), reportingExpression2.getExpression());
		return new ReportingDSLExpression(expression, mergeMeasureDependencies(reportingExpression1, reportingExpression2));
	}

	/* Multiplication */
	public static ReportingDSLExpression multiply(String measure1, String measure2) {
		Expression expression = mul(cast(getter(self(), measure1), double.class), getter(self(), measure2));
		Set<String> measureDependencies = newHashSet(measure1, measure2);
		return new ReportingDSLExpression(expression, measureDependencies);
	}

	public static ReportingDSLExpression multiply(ReportingDSLExpression reportingExpression, String measure) {
		Expression expression = mul(reportingExpression.getExpression(), cast(getter(self(), measure), double.class));
		return new ReportingDSLExpression(expression, mergeMeasureDependencies(reportingExpression, measure));
	}

	public static ReportingDSLExpression multiply(ReportingDSLExpression reportingExpression, double value) {
		Expression expression = mul(reportingExpression.getExpression(), value(value));
		return new ReportingDSLExpression(expression, reportingExpression.getMeasureDependencies());
	}

	public static ReportingDSLExpression multiply(ReportingDSLExpression reportingExpression1, ReportingDSLExpression reportingExpression2) {
		Expression expression = mul(reportingExpression1.getExpression(), reportingExpression2.getExpression());
		return new ReportingDSLExpression(expression, mergeMeasureDependencies(reportingExpression1, reportingExpression2));
	}

	/* Other functions */
	public static ReportingDSLExpression sqrt(ReportingDSLExpression reportingExpression) {
		Expression argument = reportingExpression.getExpression();
		/*
		if (Double.compare(argument, 0.0d) < 0)
			return 0.0d;
		else
			return Math.sqrt(argument);
		 */
		Expression expression = choice(cmpLt(callStatic(Double.class, "compare", argument, value(0.0d)), value(0)),
				value(0.0d),
				callStatic(Math.class, "sqrt", argument));
		return new ReportingDSLExpression(expression, reportingExpression.getMeasureDependencies());
	}

	public static ReportingDSLExpression squared(ReportingDSLExpression reportingExpression) {
		Expression expression = mul(reportingExpression.getExpression(), reportingExpression.getExpression());
		return new ReportingDSLExpression(expression, reportingExpression.getMeasureDependencies());
	}

	public static ReportingDSLExpression deviation(String value, String squaredValue, String totalValue) {
		return sqrt(subtract(divide(squaredValue, totalValue), squared(divide(value, totalValue))));
	}

	public static ReportingDSLExpression percent(ReportingDSLExpression reportingExpression) {
		return multiply(reportingExpression, 100);
	}

	public static ReportingDSLExpression preprocess(String measure, Function<Expression, Expression> function) {
		return new ReportingDSLExpression(function.apply(getter(self(), measure)), newHashSet(measure));
	}

	/* Utility methods */
	private static Set<String> mergeMeasureDependencies(ReportingDSLExpression reportingExpression1, ReportingDSLExpression reportingExpression2) {
		Set<String> measureDependencies = reportingExpression1.getMeasureDependencies();
		measureDependencies.addAll(reportingExpression2.getMeasureDependencies());
		return measureDependencies;
	}

	private static Set<String> mergeMeasureDependencies(ReportingDSLExpression reportingExpression, String measure) {
		Set<String> measureDependencies = reportingExpression.getMeasureDependencies();
		measureDependencies.add(measure);
		return measureDependencies;
	}

	private static Expression getDivisionExpression(Expression dividend, Expression divisor) {
		/*
		if (Double.compare(divisor, 0.0d) == 0)
			return 0.0d;
		else
			return dividend / divisor;
		 */
		return choice(cmpEq(callStatic(Double.class, "compare", divisor, value(0.0d)), value(0)),
				value(0.0d),
				div(dividend, divisor));
	}
}
