/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.stream.processor;

import org.junit.rules.TestRule;
import org.junit.runner.Runner;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParameters;
import org.junit.runners.parameterized.ParametersRunnerFactory;
import org.junit.runners.parameterized.TestWithParameters;

import java.util.List;

/**
 * Runner that adds essential DataKernel test rules in appropriate order
 * (for example {@link EventloopRule} must apply before any other rules)
 */
public final class DatakernelRunner extends BlockJUnit4ClassRunner {
	public DatakernelRunner(Class<?> klass) throws InitializationError {
		super(klass);
	}

	private static void addRules(List<TestRule> rules) {
		rules.add(0, new EventloopRule());
		rules.add(new ActivePromisesRule());
		rules.add(new ByteBufRule());
		rules.add(new LoggingRule());
	}

	@Override
	protected List<TestRule> getTestRules(Object target) {
		List<TestRule> rules = super.getTestRules(target);
		addRules(rules);
		return rules;
	}

	@Override
	protected String testName(FrameworkMethod method) {
		return super.testName(method);
	}

	/**
	 * For use with {@link org.junit.runners.Parameterized} runner.
	 */
	public static final class DatakernelRunnerFactory implements ParametersRunnerFactory {
		@Override
		public Runner createRunnerForTestWithParameters(TestWithParameters test) throws InitializationError {
			return new BlockJUnit4ClassRunnerWithParameters(test) {

				@Override
				protected List<TestRule> getTestRules(Object target) {
					List<TestRule> rules = super.getTestRules(target);
					addRules(rules);
					return rules;
				}
			};
		}
	}
}
