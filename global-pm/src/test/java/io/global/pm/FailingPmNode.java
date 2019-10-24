package io.global.pm;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.StacklessException;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.RawMessage;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

public final class FailingPmNode implements GlobalPmNode {
	public static final StacklessException FAILED = new StacklessException(FailingPmNode.class, "FAILED");

	@Override
	public Promise<ChannelConsumer<SignedData<RawMessage>>> upload(PubKey space, String mailBox) {
		return Promise.ofException(FAILED);
	}

	@Override
	public Promise<ChannelSupplier<SignedData<RawMessage>>> download(PubKey space, String mailBox, long timestamp) {
		return Promise.ofException(FAILED);
	}

	@Override
	public Promise<@Nullable SignedData<RawMessage>> poll(PubKey space, String mailBox) {
		return Promise.ofException(FAILED);
	}

	@Override
	public Promise<Set<String>> list(PubKey space) {
		return Promise.ofException(FAILED);
	}
}
