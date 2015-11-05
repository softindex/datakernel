package io.datakernel.rpc.client.sender;

import com.google.common.base.Optional;
import io.datakernel.rpc.client.RpcClientConnectionPool;

public interface SingleSenderStrategy {
	Optional<RequestSender> create(RpcClientConnectionPool pool);
}
