package io.datakernel.rpc.client.sender;

import com.google.common.base.Optional;
import io.datakernel.rpc.client.RpcClientConnectionPool;

import java.util.List;

import static java.util.Arrays.asList;

abstract class AbstractRequestSendingStrategy implements RequestSendingStrategy {
	AbstractRequestSendingStrategy() {

	}

	protected abstract List<Optional<RequestSender>> createAsList(RpcClientConnectionPool pool);
}
