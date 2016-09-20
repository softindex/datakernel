/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.eventloop;

/**
 * Helper class for ensuring contract between socket and code that uses this socket
 */
public final class AsyncTcpSocketContract {
	private boolean written = false;
	private boolean read = false;
	private boolean writeEndOfStream = false;
	private boolean readEndOfStream = false;
	private boolean closed = false;
	private boolean closeAndNotifyEventHandler = false;

	private AsyncTcpSocketContract() {}

	public static AsyncTcpSocketContract create() {return new AsyncTcpSocketContract();}

	// region methods
	public boolean read() {
		assert !closed : "read operation cannot be invoked when socket is closed";
		assert !readEndOfStream : "read operation cannot be invoked when end of stream was received";
		read = true;
		return true;
	}

	public boolean write() {
		assert !closed : "write operation cannot be invoked when socket is closed";
		assert !writeEndOfStream : "write operation cannot be invoked when end of stream was sent";
		written = true;
		return true;
	}

	public boolean writeEndOfStream() {
		assert !closed : "writeEndOfStream operation cannot be invoked when socket is closed";
		writeEndOfStream = true;
		written = true;
		return true;
	}

	public boolean close() {
		closed = true;
		return true;
	}

	public boolean closeAndNotifyEventHandler() {
		closed = true;
		closeAndNotifyEventHandler = true;
		return true;
	}
	// endregion

	// region callbacks
	public boolean onRead() {
		assert !closed : "onRead callback cannot be invoked when socket is closed";
		assert !readEndOfStream : "onRead callback cannot be invoked when end of stream was received";
		assert read : "onRead callback cannot be invoked if read operation was not requested";
		read = false;
		return true;
	}

	public boolean onReadEndOfStream() {
		assert !closed : "onReadEndOfStream callback cannot be invoked when socket is closed";
		assert !readEndOfStream :
				"onReadEndOfStream callback cannot be invoked when end of stream was already received";
		readEndOfStream = true;
		return true;
	}

	public boolean onWrite() {
		assert !closed : "onWrite callback cannot be invoked when socket is closed";
		assert written : "onWrite callback cannot be invoked if write operation was not called before";
		written = false;
		return true;
	}

	public boolean onClosedWithError() {
		assert !closed || closeAndNotifyEventHandler :
				"onClosedWithError callback cannot be invoked when socket is already closed";
		closed = true;
		closeAndNotifyEventHandler = false;
		return true;
	}
	// endregion
}
