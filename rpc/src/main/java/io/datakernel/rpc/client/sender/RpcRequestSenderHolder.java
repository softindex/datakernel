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

package io.datakernel.rpc.client.sender;

import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

public final class RpcRequestSenderHolder {
	private final RpcRequestSender sender;

	private RpcRequestSenderHolder(RpcRequestSender sender) {
		this.sender = sender;
	}

	/**
	 * Creates instance of {@link RpcRequestSenderHolder} with specified {@code sender}
	 *
	 * @param sender {@link RpcRequestSender} to be placed in {@link RpcRequestSenderHolder}
	 * @return instance of {@link RpcRequestSenderHolder} with specified {@code sender}
	 * @throws NullPointerException if {@code sender} is null
	 */
	public static RpcRequestSenderHolder of(RpcRequestSender sender) throws NullPointerException {
		checkNotNull(sender);
		return new RpcRequestSenderHolder(sender);
	}

	/**
	 * Creates instance of {@link RpcRequestSenderHolder} with no sender
	 *
	 * @return instance of {@link RpcRequestSenderHolder} with no sender
	 */
	public static RpcRequestSenderHolder absent() {
		return new RpcRequestSenderHolder(null);
	}

	/**
	 * Checks whether sender is present in this holder
	 *
	 * @return {@code true} if there is a sender, and {@code false} otherwise
	 */
	public boolean isSenderPresent() {
		return sender != null;
	}

	/**
	 * Returns sender if it was specified, otherwise throws exception
	 *
	 * @return {@link RpcRequestSender} which was specified
	 * @throws IllegalStateException if there is no sender
	 */
	public RpcRequestSender getSender() throws IllegalStateException {
		checkState(sender != null, "There is no sender in holder");
		return sender;
	}

	/**
	 * Returns sender if it was specified, otherwise returns null
	 *
	 * @return {@link RpcRequestSender} if it was specified, otherwise null
	 */
	public RpcRequestSender getSenderOrNull() {
		return sender;
	}
}
