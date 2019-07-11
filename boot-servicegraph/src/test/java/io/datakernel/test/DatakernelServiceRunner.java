package io.datakernel.test;

import io.datakernel.service.ServiceGraph;
import io.datakernel.test.rules.LambdaStatement;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

public class DatakernelServiceRunner extends DatakernelRunner {
	private ServiceGraph currentServiceGraph;

	public DatakernelServiceRunner(Class<?> clazz) throws InitializationError {
		super(clazz);
	}

	@Override
	protected Statement withBefores(FrameworkMethod method, Object target, Statement test) {
		return super.withBefores(method, target, new LambdaStatement(() -> {
			currentServiceGraph = currentInjector.getInstanceOrNull(ServiceGraph.class);
			if (currentServiceGraph != null) {
				currentServiceGraph.startFuture().get();
			}
			test.evaluate();
		}));
	}

	@Override
	protected Statement withAfters(FrameworkMethod method, Object target, Statement test) {
		return super.withAfters(method, target, new LambdaStatement(() -> {
			test.evaluate();
			if (currentServiceGraph != null) {
				currentServiceGraph.stopFuture().get();
			}
		}));
	}
}
