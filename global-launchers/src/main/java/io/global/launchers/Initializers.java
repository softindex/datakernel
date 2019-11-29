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

package io.global.launchers;

import io.datakernel.common.Initializer;
import io.datakernel.config.Config;
import io.datakernel.http.AsyncHttpServer;
import io.global.common.api.AbstractGlobalNamespace;
import io.global.common.api.AbstractGlobalNode;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Executor;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.launchers.initializers.Initializers.ofHttpWorker;
import static io.global.common.api.AbstractGlobalNode.DEFAULT_LATENCY_MARGIN;
import static io.global.launchers.GlobalConfigConverters.ofPubKey;
import static io.global.launchers.Utils.getSslContext;
import static java.util.Collections.emptyList;

public class Initializers {
	private Initializers() {
		throw new AssertionError();
	}

	public static <S extends AbstractGlobalNode<S, L, N>, L extends AbstractGlobalNamespace<L, S, N>, N> Initializer<S>
	ofAbstractGlobalNode(Config config) {
		return node -> node
				.withLatencyMargin(config.get(ofDuration(), "latencyMargin", DEFAULT_LATENCY_MARGIN))
				.withManagedPublicKeys(new HashSet<>(config.get(ofList(ofPubKey()), "managedKeys", emptyList())));
	}

	public static Initializer<AsyncHttpServer> sslServerInitializer(Executor executor, Config config) {
		List<Path> certPaths = config.get(ofList(ofPath()), "certificate", null);
		Path privateKeyPath = config.get(ofPath(), "privateKey", null);
		SSLContext sslContext;
		try {
			if (certPaths != null && privateKeyPath != null) {
				sslContext = getSslContext(certPaths, privateKeyPath);
			} else {
				sslContext = null;
			}
		} catch (GeneralSecurityException | IOException e) {
			throw new RuntimeException(e);
		}
		return ofHttpWorker(config)
				.andThen(server -> {
					server.withAcceptOnce(config.get(ofBoolean(), "acceptOnce", false))
							.withSocketSettings(config.get(ofSocketSettings(), "socketSettings", server.getSocketSettings()))
							.withServerSocketSettings(config.get(ofServerSocketSettings(), "serverSocketSettings", server.getServerSocketSettings()));
					List<InetSocketAddress> listenAddresses = config.get(ofList(ofInetSocketAddress()), "listenAddresses");
					if (sslContext == null) {
						server.withListenAddresses(listenAddresses);
					} else {
						server.withSslListenAddresses(sslContext, executor, listenAddresses);
					}
				});
	}
}
