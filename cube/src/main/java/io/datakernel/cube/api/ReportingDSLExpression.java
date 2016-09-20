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

import com.google.common.base.MoreObjects;
import io.datakernel.codegen.Expression;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public final class ReportingDSLExpression {
	private final Expression expression;
	private final Set<String> measureDependencies;

	private ReportingDSLExpression(Expression expression, Set<String> measureDependencies) {
		this.expression = expression;
		this.measureDependencies = measureDependencies;
	}

	public static ReportingDSLExpression create(Expression expression, Set<String> measureDependencies) {
		return new ReportingDSLExpression(expression, measureDependencies);
	}

	public Expression getExpression() {
		return expression;
	}

	public Set<String> getMeasureDependencies() {
		return new HashSet<>(measureDependencies);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ReportingDSLExpression that = (ReportingDSLExpression) o;
		return Objects.equals(expression, that.expression) &&
				Objects.equals(measureDependencies, that.measureDependencies);
	}

	@Override
	public int hashCode() {
		return Objects.hash(expression, measureDependencies);
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("expression", expression)
				.add("measureDependencies", measureDependencies)
				.toString();
	}
}
