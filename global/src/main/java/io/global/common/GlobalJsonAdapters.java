/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.global.common;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.global.common.api.AnnounceData;
import io.global.fs.api.GlobalFsMetadata;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.datakernel.json.GsonAdapters.*;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.lang.Integer.parseInt;

public final class GlobalJsonAdapters {
	private GlobalJsonAdapters() {
		throw new AssertionError("nope.");
	}

	public static final TypeAdapter<PrivKey> PRIV_KEY = transform(STRING_JSON, PrivKey::fromString, PrivKey::asString);
	public static final TypeAdapter<PubKey> PUB_KEY = transform(STRING_JSON, PubKey::fromString, PubKey::asString);
	public static final TypeAdapter<Signature> SIGNATURE = transform(STRING_JSON, Signature::fromString, Signature::asString);
	public static final TypeAdapter<Hash> HASH = transform(STRING_JSON, Hash::fromString, Hash::asString);

	@SuppressWarnings("unchecked")
	public static <T extends ByteArrayIdentity> TypeAdapter<SignedData<T>> withSignarure(TypeAdapter<T> adapter) {
		Map<String, TypeAdapter<?>> props = new HashMap<>();
		props.put("data", adapter);
		props.put("signature", SIGNATURE);

		return transform(ofHeterogeneousMap(props),
				data -> SignedData.of((T) data.get("data"), (Signature) data.get("signature")),
				signedData -> {
					Map<String, Object> map = new HashMap<>();
					map.put("data", signedData.getData());
					map.put("signature", signedData.getSignature());
					return map;
				});
	}

	// region GlobalPath
	private static final Map<String, TypeAdapter<?>> GLOBAL_PATH_PROPS = new HashMap<>();

	static {
		GLOBAL_PATH_PROPS.put("owner", PUB_KEY);
		GLOBAL_PATH_PROPS.put("fs", STRING_JSON);
		GLOBAL_PATH_PROPS.put("path", STRING_JSON);
	}

	// region LocalPath
	private static final Map<String, TypeAdapter<?>> LOCAL_PATH_PROPS = new HashMap<>();

	static {
		LOCAL_PATH_PROPS.put("fs", STRING_JSON);
		LOCAL_PATH_PROPS.put("path", STRING_JSON);
	}

	// region GlobalFsMetadata
	private static final Map<String, TypeAdapter<?>> GLOBAL_FS_METADATA_PROPS = new HashMap<>();

	static {
		GLOBAL_FS_METADATA_PROPS.put("filename", STRING_JSON);
		GLOBAL_FS_METADATA_PROPS.put("size", LONG_JSON);
		GLOBAL_FS_METADATA_PROPS.put("revision", LONG_JSON);
	}

	public static final TypeAdapter<GlobalFsMetadata> GLOBAL_FS_METADATA =
			transform(ofHeterogeneousMap(GLOBAL_FS_METADATA_PROPS),
					data -> GlobalFsMetadata.of((String) data.get("filename"), (Long) data.get("size"), (Long) data.get("revision")),
					meta -> {
						Map<String, Object> map = new HashMap<>();
						map.put("filename", meta.getFilename());
						map.put("size", meta.getSize());
						map.put("revision", meta.getRevision());
						return map;
					});
	// endregion

	// region InetSocketAddress
	public static final TypeAdapter<InetSocketAddress> INET_SOCKET_ADDRESS = new TypeAdapter<InetSocketAddress>() {
		@Override
		public void write(JsonWriter out, InetSocketAddress value) throws IOException {
			out.value(value.getAddress().getHostAddress() + ":" + value.getPort());
		}

		@Override
		public InetSocketAddress read(JsonReader in) throws IOException {
			String addressPort = in.nextString();
			int portPos = addressPort.lastIndexOf(':');
			if (portPos == -1) {
				return new InetSocketAddress(parseInt(addressPort));
			}
			String addressStr = addressPort.substring(0, portPos);
			String portStr = addressPort.substring(portPos + 1);
			int port = parseInt(portStr);
			checkArgument(port > 0 && port < 65536, "Invalid address. Port is not in range (0, 65536) " + addressStr);
			InetSocketAddress socketAddress;

			//noinspection Duplicates - this piece of code is stolen from ConfigConverters#ofInetSocketAddress
			if ("*".equals(addressStr)) {
				socketAddress = new InetSocketAddress(port);
			} else {
				try {
					InetAddress address = InetAddress.getByName(addressStr);
					socketAddress = new InetSocketAddress(address, port);
				} catch (UnknownHostException e) {
					throw new IllegalArgumentException(e);
				}
			}
			return socketAddress;
		}
	};
	// endregion

	public static final TypeAdapter<RawServerId> RAW_SERVER_ID = transform(INET_SOCKET_ADDRESS, RawServerId::new, RawServerId::getInetSocketAddress);

	// region AnnounceData
	private static final Map<String, TypeAdapter<?>> ANNOUNCE_DATA_PROPS = new HashMap<>();

	static {
		ANNOUNCE_DATA_PROPS.put("timestamp", LONG_JSON);
		ANNOUNCE_DATA_PROPS.put("servers", ofSet(RAW_SERVER_ID));
	}

	@SuppressWarnings("unchecked")
	public static final TypeAdapter<AnnounceData> ANNOUNCE_DATA =
			transform(ofHeterogeneousMap(ANNOUNCE_DATA_PROPS),
					data -> AnnounceData.of((Long) data.get("timestamp"), (Set<RawServerId>) data.get("servers")),
					announceData -> {
						Map<String, Object> map = new HashMap<>();
						map.put("timestamp", announceData.getTimestamp());
						map.put("servers", announceData.getServerIds());
						return map;
					});
	// endregion

	// region SharedSimKey
	private static final Map<String, TypeAdapter<?>> SHARED_SIM_KEY_PROPS = new HashMap<>();

	static {
		SHARED_SIM_KEY_PROPS.put("receiver", PUB_KEY);
		SHARED_SIM_KEY_PROPS.put("hash", HASH);
		SHARED_SIM_KEY_PROPS.put("encryptedSimKey", BYTES_JSON);
	}

	public static final TypeAdapter<SharedSimKey> SHARED_SIM_KEY =
			transform(ofHeterogeneousMap(SHARED_SIM_KEY_PROPS),
					data -> SharedSimKey.of((PubKey) data.get("receiver"), (Hash) data.get("hash"), (byte[]) data.get("encryptedSimKey")),
					sharedSimKey -> {
						Map<String, Object> map = new HashMap<>();
						map.put("receiver", sharedSimKey.getReceiver());
						map.put("hash", sharedSimKey.getHash());
						map.put("encryptedSimKey", sharedSimKey.getEncryptedSimKey());
						return map;
					});
	// endregion
}
