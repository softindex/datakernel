package io.datakernel.test.rules;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static io.datakernel.util.Preconditions.checkNotNull;

public class ExecutorRule implements TestRule {
	private static final ThreadLocal<ExecutorService> threadLocal = new ThreadLocal<>();
	private static final Supplier<ExecutorService> DEFAULT_EXECUTOR = Executors::newCachedThreadPool;
	private static final ThreadLocal<Integer> refs = new ThreadLocal<>();
	private static final Integer FIRST = 1;

	public ExecutorRule() {
		this(DEFAULT_EXECUTOR.get());
	}

	public ExecutorRule(ExecutorService executorService) {
		Integer countRefsToExecutor = refs.get();
		if (countRefsToExecutor == null) {
			threadLocal.set(executorService);
			refs.set(FIRST);
		} else {
			refs.set(++countRefsToExecutor);
		}
	}

	public static Executor getExecutor() {
		return checkNotNull(threadLocal.get(), "ExecutorRule is not initialized");
	}

	int i;
	@Override
	public Statement apply(Statement base, Description description) {
		return new LambdaStatement(() -> {
			base.evaluate();
			ExecutorService executorService = threadLocal.get();
			Integer countRefs = refs.get();
			if (countRefs != null && executorService != null) {
				if (--countRefs == 0) {
					threadLocal.remove();
					refs.remove();
					executorService.shutdown();
				} else {
					refs.set(countRefs);
				}
			}
		});
	}
}
