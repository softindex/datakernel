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

package io.datakernel.rpc.protocol;

import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.serializer.annotations.SerializeNullable;
import io.datakernel.serializer.annotations.SerializeSubclasses;

public final class RpcMessage {
	@SerializeSubclasses(extraSubclassesId = "extraRpcMessages", value = {RpcRemoteException.class})
	public interface RpcMessageData { // TODO (vmykhalko): remove this interface, change to Object
		boolean isMandatory();
	}

	public static abstract class AbstractMandatoryRpcMessage implements RpcMessageData {
		@Override
		public boolean isMandatory() {
			return true;
		}
	}

	public static abstract class AbstractRpcMessage implements RpcMessageData {
		@Override
		public boolean isMandatory() {
			return false;
		}
	}

	private final int cookie;
	private final RpcMessageData data;

	public RpcMessage(@Deserialize("cookie") int cookie, @Deserialize("data") RpcMessageData data) {
		this.cookie = cookie;
		this.data = data;
	}

	@Serialize(order = 1)
	public int getCookie() {
		return cookie;
	}

	@Serialize(order = 2)
	@SerializeNullable
	public RpcMessageData getData() {
		return data;
	}
}
