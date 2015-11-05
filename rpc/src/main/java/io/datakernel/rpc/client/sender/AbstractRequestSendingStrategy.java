package io.datakernel.rpc.client.sender;

import com.google.common.base.Optional;
import io.datakernel.rpc.client.RpcClientConnectionPool;

import java.util.List;

public abstract class AbstractRequestSendingStrategy implements RequestSendingStrategy {
	AbstractRequestSendingStrategy() {

	}

	protected abstract List<Optional<RequestSender>> createAsList(RpcClientConnectionPool pool);
}
