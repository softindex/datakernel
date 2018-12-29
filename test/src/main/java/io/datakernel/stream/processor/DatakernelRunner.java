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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.datakernel.eventloop.Eventloop;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;
import org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParameters;
import org.junit.runners.parameterized.ParametersRunnerFactory;
import org.junit.runners.parameterized.TestWithParameters;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.List;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

/**
 * Runner that adds essential DataKernel test rules and runs the eventloop.
 * <p>
 * Also handles the &#064;Manual annotation.
 */
public final class DatakernelRunner extends BlockJUnit4ClassRunner {
	private boolean manualRun;

	public DatakernelRunner(Class<?> klass) throws InitializationError {
		super(klass);
	}

	private static List<TestRule> addRules(List<TestRule> superRules) {
		superRules.add(new ActivePromisesRule());
		superRules.add(new ByteBufRule());
		superRules.add(new LoggingRule());
		return superRules;
	}

	private static void removeEmptySuiteError(List<Throwable> errors) {
		// allow empty test classes (e.g. when whole class is @Manual)
		errors.removeIf(e -> e.getClass() == Exception.class && "No runnable methods".equals(e.getMessage()));
	}

	private static Statement addEventloop(Statement root) {
		Eventloop.create()
				.withCurrentThread()
				.withFatalErrorHandler(rethrowOnAnyError());

		// set eventloop logger level to WARN so that await calls do not spam with eventloop running and finishing each time
		((Logger) LoggerFactory.getLogger(Eventloop.class.getName())).setLevel(Level.WARN);

		return new LambdaStatement(root::evaluate);
	}

	private static Description getModifiedDescription(TestClass testClass, Description superDescription, String name, Annotation[] runnerAnnotations) {
		Description description = Description.createSuiteDescription(name, runnerAnnotations);
		if (testClass.getAnnotation(Manual.class) == null) {
			for (Description child : superDescription.getChildren()) {
				if (child.getAnnotation(Manual.class) == null) {
					description.addChild(child);
				}
			}
		}
		return description;
	}

	private static Filter removeManualTests() {
		// hacky way to remove tests with @Manual from the filteredChildren list
		return new Filter() {
			@Override
			public boolean shouldRun(Description description) {
				return description.getAnnotation(Manual.class) == null
						&& description.getTestClass().getAnnotation(Manual.class) == null;
			}

			@Override
			public String describe() {
				return "all tests except manual";
			}
		};
	}

	private static boolean isManualRun(Filter filter) {
		// quite a dirty hack, but it works
		String name = filter.getClass().getName();
		return name.equals("org.junit.runner.manipulation.Filter$2") || name.equals("com.intellij.junit4.JUnit4TestRunnerUtil$3");
	}

	@Override
	protected List<TestRule> getTestRules(Object target) {
		return addRules(super.getTestRules(target));
	}

	@Override
	protected Statement methodInvoker(FrameworkMethod method, Object test) {
		return addEventloop(super.methodInvoker(method, test));
	}

	@Override
	protected void collectInitializationErrors(List<Throwable> errors) {
		super.collectInitializationErrors(errors);
		removeEmptySuiteError(errors);
	}

	@Override
	public Description getDescription() {
		return manualRun ? super.getDescription() : getModifiedDescription(getTestClass(), super.getDescription(), getName(), getRunnerAnnotations());
	}

	@Override
	public void run(RunNotifier notifier) {
		if (!manualRun) {
			try {
				super.filter(removeManualTests());
			} catch (NoTestsRemainException ignored) {
			}
		}
		super.run(notifier);
	}

	@Override
	public void filter(Filter filter) throws NoTestsRemainException {
		manualRun |= isManualRun(filter);
		super.filter(filter);
	}

	@Override
	protected boolean isIgnored(FrameworkMethod child) {
		// allow manually running ignored tests like with IntelliJ's default runner does
		return super.isIgnored(child) && !manualRun;
	}

	/**
	 * For use with {@link org.junit.runners.Parameterized} runner.
	 */
	public static final class DatakernelRunnerFactory implements ParametersRunnerFactory {
		@Override
		public Runner createRunnerForTestWithParameters(TestWithParameters test) throws InitializationError {
			return new BlockJUnit4ClassRunnerWithParameters(test) {
				private boolean manualRun;

				@Override
				protected List<TestRule> getTestRules(Object target) {
					return addRules(super.getTestRules(target));
				}

				@Override
				protected Statement methodInvoker(FrameworkMethod method, Object test) {
					return addEventloop(super.methodInvoker(method, test));
				}

				@Override
				protected void collectInitializationErrors(List<Throwable> errors) {
					super.collectInitializationErrors(errors);
					removeEmptySuiteError(errors);
				}

				@Override
				public Description getDescription() {
					return manualRun ? super.getDescription() : getModifiedDescription(getTestClass(), super.getDescription(), getName(), getRunnerAnnotations());
				}

				@Override
				public void run(RunNotifier notifier) {
					if (!manualRun) {
						try {
							super.filter(removeManualTests());
						} catch (NoTestsRemainException ignored) {
						}
					}
					super.run(notifier);
				}

				@Override
				public void filter(Filter filter) throws NoTestsRemainException {
					manualRun |= isManualRun(filter);
					super.filter(filter);
				}

				@Override
				protected boolean isIgnored(FrameworkMethod child) {
					return super.isIgnored(child) && !manualRun;
				}
			};
		}
	}

}
