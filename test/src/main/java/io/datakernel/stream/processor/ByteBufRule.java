package io.datakernel.stream.processor;

import io.datakernel.bytebuf.ByteBufPool;
import org.junit.rules.ExternalResource;

import static org.junit.Assert.assertEquals;

public final class ByteBufRule extends ExternalResource {
	private boolean enabled = true;

	public void enable(boolean enabled) {
		this.enabled = enabled;
	}

	@Override
	protected void before() {
		ByteBufPool.clear();
	}

	@Override
	protected void after() {
		if (enabled) {
			assertEquals(ByteBufPool.getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
		}
	}
}
