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

package io.datakernel.aggregation_db.api;

import io.datakernel.codegen.Expression;

import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.codegen.Expressions.*;

public final class ReportingDSL {
	private ReportingDSL() {}

	public static ReportingDSLExpression sqrt(ReportingDSLExpression reportingExpression) {
		Expression expression = callStatic(Math.class, "sqrt", reportingExpression.getExpression());
		return new ReportingDSLExpression(expression, reportingExpression.getMeasureDependencies());
	}

	public static ReportingDSLExpression subtract(ReportingDSLExpression reportingExpression1, ReportingDSLExpression reportingExpression2) {
		Expression expression = sub(reportingExpression1.getExpression(), reportingExpression2.getExpression());
		return new ReportingDSLExpression(expression, mergeMeasureDependencies(reportingExpression1, reportingExpression2));
	}

	public static ReportingDSLExpression divide(String measure1, String measure2) {
		Expression expression = div(cast(field(self(), measure1), double.class), field(self(), measure2));
		Set<String> measureDependencies = newHashSet(measure1, measure2);
		return new ReportingDSLExpression(expression, measureDependencies);
	}

	public static ReportingDSLExpression divide(ReportingDSLExpression reportingExpression, String measure) {
		Expression expression = div(reportingExpression.getExpression(), cast(field(self(), measure), double.class));
		Set<String> measureDependencies = reportingExpression.getMeasureDependencies();
		measureDependencies.add(measure);
		return new ReportingDSLExpression(expression, measureDependencies);
	}

	public static ReportingDSLExpression multiply(String measure1, String measure2) {
		Expression expression = mul(cast(field(self(), measure1), double.class), field(self(), measure2));
		Set<String> measureDependencies = newHashSet(measure1, measure2);
		return new ReportingDSLExpression(expression, measureDependencies);
	}

	public static ReportingDSLExpression multiply(ReportingDSLExpression reportingExpression, double value) {
		Expression expression = mul(reportingExpression.getExpression(), value(value));
		return new ReportingDSLExpression(expression, reportingExpression.getMeasureDependencies());
	}

	public static ReportingDSLExpression multiply(ReportingDSLExpression reportingExpression1, ReportingDSLExpression reportingExpression2) {
		Expression expression = mul(reportingExpression1.getExpression(), reportingExpression2.getExpression());
		return new ReportingDSLExpression(expression, mergeMeasureDependencies(reportingExpression1, reportingExpression2));
	}

	private static Set<String> mergeMeasureDependencies(ReportingDSLExpression reportingExpression1, ReportingDSLExpression reportingExpression2) {
		Set<String> measureDependencies = reportingExpression1.getMeasureDependencies();
		measureDependencies.addAll(reportingExpression2.getMeasureDependencies());
		return measureDependencies;
	}
}
