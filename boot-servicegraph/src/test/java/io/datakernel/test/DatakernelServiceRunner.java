package io.datakernel.test;

import io.datakernel.service.ServiceGraph;
import io.datakernel.test.rules.LambdaStatement;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

public class DatakernelServiceRunner extends DatakernelRunner {
	public DatakernelServiceRunner(Class<?> clazz) throws InitializationError {
		super(clazz);
	}

	@Override
	protected Statement methodInvoker(FrameworkMethod method, Object test) {
		return new LambdaStatement(() -> {
			// create args before running the service graph so that those args that are services are found by service graph
			Object[] args = getArgs(method);

			ServiceGraph serviceGraph = currentInjector.getInstanceOrNull(ServiceGraph.class);

			if (serviceGraph == null) {
				method.invokeExplosively(test, args);
				return;
			}

			serviceGraph.startFuture().get();

			method.invokeExplosively(test, args);

			serviceGraph.stopFuture().get();
		});
	}
}
