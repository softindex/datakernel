package io.datakernel.rpc.client.sender.helper;

import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.protocol.RpcMessage;
import static io.datakernel.rpc.protocol.RpcMessage.RpcMessageData;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import java.util.concurrent.atomic.AtomicInteger;

public class RpcClientConnectionStub implements RpcClientConnection {

	private AtomicInteger calls;

	public RpcClientConnectionStub() {
		calls = new AtomicInteger(0);
	}

	public int getCallsAmount() {
		return calls.get();
	}

	@Override
	public <I extends RpcMessageData, O extends RpcMessageData> void callMethod(I request,
	                                                                            int timeout,
	                                                                            ResultCallback<O> callback) {
		calls.incrementAndGet();
	}

	@Override
	public void close() {
		throw new UnsupportedOperationException();
	}

	@Override
	public SocketConnection getSocketConnection() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void onReceiveMessage(RpcMessage message) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void ready() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void onClosed() {
		throw new UnsupportedOperationException();
	}

	@Override
	public NioEventloop getEventloop() {
		throw new UnsupportedOperationException();
	}




	// JMX

	@Override
	public void startMonitoring() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void stopMonitoring() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isMonitoring() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void reset() {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompositeData getConnectionDetails() throws OpenDataException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getPendingRequests() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getSuccessfulRequests() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getFailedRequests() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getRejectedRequests() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getExpiredRequests() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getPendingRequestsStats() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getProcessResultTimeStats() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getProcessExceptionTimeStats() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getSendPacketTimeStats() {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompositeData getLastTimeoutException() {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompositeData getLastProtocolException() {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompositeData getLastRemoteException() {
		throw new UnsupportedOperationException();
	}
}
