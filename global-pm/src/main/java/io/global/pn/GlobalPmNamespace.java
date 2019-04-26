package io.global.pn;

import io.datakernel.async.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AbstractGlobalNamespace;
import io.global.pn.api.GlobalPmNode;
import io.global.pn.api.RawMessage;
import org.jetbrains.annotations.Nullable;

public final class GlobalPmNamespace extends AbstractGlobalNamespace<GlobalPmNamespace, GlobalPmNodeImpl, GlobalPmNode> {
	public GlobalPmNamespace(GlobalPmNodeImpl node, PubKey space) {
		super(node, space);
	}

	public Promise<Void> send(String mailBox, SignedData<RawMessage> message) {
		return node.getStorage().store(space, mailBox, message);
	}

	public Promise<@Nullable SignedData<RawMessage>> poll(String mailBox) {
		return node.getStorage().load(space, mailBox);
	}

	public Promise<Void> drop(String mailBox, SignedData<Long> id) {
		return node.getStorage().delete(space, mailBox, id.getValue());
	}
}
