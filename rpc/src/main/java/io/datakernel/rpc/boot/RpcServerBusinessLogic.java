package io.datakernel.rpc.boot;

import io.datakernel.rpc.server.RpcServer;

import java.util.function.Consumer;

/**
 * Logic for RPC server expressed via configuring lambda.
 */
@FunctionalInterface
interface RpcServerBusinessLogic extends Consumer<RpcServer> {
}
