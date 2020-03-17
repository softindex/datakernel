package io.global.pm;

import io.datakernel.csp.ChannelSupplier;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.RawMessage;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.common.Preconditions.checkNotNull;
import static io.datakernel.common.Preconditions.checkState;

public final class PmMasterRepository {
	private final RawServerId serverId;
	private final PubKey pubKey;
	private final String mailBox;
	private final GlobalPmNode node;

	@Nullable
	private ChannelSupplier<SignedData<RawMessage>> stream;

	public PmMasterRepository(RawServerId serverId, PubKey pubKey, String mailBox, GlobalPmNode node) {
		this.serverId = serverId;
		this.pubKey = pubKey;
		this.mailBox = mailBox;
		this.node = node;
	}

	public RawServerId getServerId() {
		return serverId;
	}

	public PubKey getPubKey() {
		return pubKey;
	}

	public String getMailBox() {
		return mailBox;
	}

	public GlobalPmNode getNode() {
		return node;
	}

	public ChannelSupplier<SignedData<RawMessage>> getStream() {
		return checkNotNull(stream);
	}

	public ChannelSupplier<SignedData<RawMessage>> createStream(long fromTimestamp) {
		checkState(stream == null);
		stream = ChannelSupplier.ofPromise(node.stream(pubKey, mailBox, fromTimestamp));
		return stream;
	}

	public void closeStream() {
		if (stream != null) {
			stream.close();
		}
	}
}
