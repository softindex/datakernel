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

import java.util.List;

import static io.datakernel.codegen.Expressions.*;
import static java.util.Arrays.asList;

public final class ReportingDSL {
	private ReportingDSL() {}

	public static ReportingDSLExpression divide(String measure1, String measure2) {
		Expression expression = div(cast(field(self(), measure1), double.class), field(self(), measure2));
		List<String> measureDependencies = asList(measure1, measure2);
		return new ReportingDSLExpression(expression, measureDependencies);
	}

	public static ReportingDSLExpression divide(ReportingDSLExpression reportingExpression, String measure) {
		Expression expression = div(reportingExpression.getExpression(), cast(field(self(), measure), double.class));
		List<String> measureDependencies = reportingExpression.getMeasureDependencies();
		measureDependencies.add(measure);
		return new ReportingDSLExpression(expression, measureDependencies);
	}

	public static ReportingDSLExpression multiply(String measure1, String measure2) {
		Expression expression = mul(cast(field(self(), measure1), double.class), field(self(), measure2));
		List<String> measureDependencies = asList(measure1, measure2);
		return new ReportingDSLExpression(expression, measureDependencies);
	}

	public static ReportingDSLExpression multiply(ReportingDSLExpression reportingExpression, double value) {
		Expression expression = mul(reportingExpression.getExpression(), value(value));
		return new ReportingDSLExpression(expression, reportingExpression.getMeasureDependencies());
	}
}
