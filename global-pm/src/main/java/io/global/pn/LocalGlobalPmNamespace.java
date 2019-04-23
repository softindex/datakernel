package io.global.pn;

import io.datakernel.async.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AbstractGlobalNamespace;
import io.global.pn.api.GlobalPmNode;
import io.global.pn.api.RawMessage;
import org.jetbrains.annotations.Nullable;

public final class LocalGlobalPmNamespace extends AbstractGlobalNamespace<LocalGlobalPmNamespace, LocalGlobalPmNode, GlobalPmNode> {
	public LocalGlobalPmNamespace(LocalGlobalPmNode node, PubKey space) {
		super(node, space);
	}

	public Promise<Void> send(SignedData<RawMessage> message) {
		return node.getStorage().store(space, message);
	}

	public Promise<@Nullable SignedData<RawMessage>> poll() {
		return node.getStorage().load(space);
	}

	public Promise<Void> drop(SignedData<Long> id) {
		return node.getStorage().delete(space, id.getValue());
	}
}
