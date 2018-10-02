package io.datakernel.stream.processor;

import io.datakernel.bytebuf.ByteBufPool;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static org.junit.Assert.assertEquals;

public final class ByteBufRule implements TestRule {
	static {
		System.setProperty("ByteBufPool.minSize", "1");
		System.setProperty("ByteBufPool.maxSize", "0");
	}

	private boolean enabled = true;

	public void enable(boolean enabled) {
		this.enabled = enabled;
	}

	@Override
	public Statement apply(Statement base, Description description) {
		return new Statement() {
			@Override
			public void evaluate() throws Throwable {
				ByteBufPool.clear();
				base.evaluate();
				if (enabled) {
					assertEquals(ByteBufPool.getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
				}
			}
		};
	}
}
