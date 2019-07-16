package io.global.pm;

import io.datakernel.async.Promise;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AbstractGlobalNode;
import io.global.common.api.DiscoveryService;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.MessageStorage;
import io.global.pm.api.RawMessage;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

public final class GlobalPmNodeImpl extends AbstractGlobalNode<GlobalPmNodeImpl, GlobalPmNamespace, GlobalPmNode> implements GlobalPmNode {

	private final MessageStorage storage;

	private GlobalPmNodeImpl(RawServerId id, DiscoveryService discoveryService,
							 Function<RawServerId, GlobalPmNode> nodeFactory,
							 MessageStorage storage) {
		super(id, discoveryService, nodeFactory);
		this.storage = storage;
	}

	public static GlobalPmNodeImpl create(RawServerId id, DiscoveryService discoveryService,
			Function<RawServerId, GlobalPmNode> nodeFactory, MessageStorage storage) {
		return new GlobalPmNodeImpl(id, discoveryService, nodeFactory, storage);
	}

	@Override
	protected GlobalPmNamespace createNamespace(PubKey space) {
		return new GlobalPmNamespace(this, space);
	}

	public MessageStorage getStorage() {
		return storage;
	}

	@Override
	public @NotNull Promise<Void> send(PubKey space, String mailBox, SignedData<RawMessage> message) {
		return simpleMethod(space, node -> node.send(space, mailBox, message), ns -> ns.send(mailBox, message));
	}

	@Override
	public @NotNull Promise<SignedData<RawMessage>> poll(PubKey space, String mailBox) {
		return simpleMethod(space, node -> node.poll(space, mailBox), ns -> ns.poll(mailBox));
	}

	@Override
	public Promise<Void> drop(PubKey space, String mailBox, SignedData<Long> id) {
		return simpleMethod(space, node -> node.drop(space, mailBox, id), ns -> ns.drop(mailBox, id));
	}
}
