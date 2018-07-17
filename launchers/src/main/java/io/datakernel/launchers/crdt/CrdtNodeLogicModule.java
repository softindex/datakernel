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

package io.datakernel.launchers.crdt;

import com.google.inject.*;
import io.datakernel.config.Config;
import io.datakernel.crdt.CrdtClient;
import io.datakernel.crdt.CrdtClusterClient;
import io.datakernel.crdt.CrdtRepartitionController;
import io.datakernel.crdt.CrdtServer;
import io.datakernel.crdt.local.FsCrdtClient;
import io.datakernel.crdt.local.RuntimeCrdtClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.FsClient;
import io.datakernel.util.RecursiveType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import static io.datakernel.launchers.Initializers.*;
import static java.util.Collections.singletonMap;

public abstract class CrdtNodeLogicModule<K extends Comparable<K>, S> extends AbstractModule {

	@SuppressWarnings("unchecked")
	@Override
	protected void configure() {
		Type genericSuperclass = getClass().getGenericSuperclass();
		Type[] typeArguments = ((ParameterizedType) genericSuperclass).getActualTypeArguments();
		RecursiveType k = RecursiveType.of(typeArguments[0]);
		RecursiveType s = RecursiveType.of(typeArguments[1]);
		Type clientType = RecursiveType.of(CrdtClient.class, k, s).getType();

		bind((Key) Key.get(clientType, InMemory.class))
				.to(Key.get(RecursiveType.of(RuntimeCrdtClient.class, k, s).getType()));
		bind((Key) Key.get(clientType, Persistent.class))
				.to(Key.get(RecursiveType.of(FsCrdtClient.class, k, s).getType()));
		bind((Key) Key.get(clientType, Cluster.class))
				.to(Key.get(RecursiveType.of(CrdtClusterClient.class, RecursiveType.of(String.class), k, s).getType()));
	}

	@Provides
	@Singleton
	RuntimeCrdtClient<K, S> provideRuntimeCrdtClient(Eventloop eventloop, CrdtDescriptor<K, S> descriptor) {
		return RuntimeCrdtClient.create(eventloop, descriptor.getCombiner());
	}

	@Provides
	@Singleton
	FsCrdtClient<K, S> provideFsCrdtClient(Eventloop eventloop, Config config, FsClient fsClient, CrdtDescriptor<K, S> descriptor) {
		return FsCrdtClient.create(eventloop, fsClient, descriptor.getCombiner(), descriptor.getSerializer())
				.initialize(ofFsCrdtClient(config));
	}

	@Provides
	@Singleton
	CrdtClusterClient<String, K, S> provideClusterCrdtClient(Config config, RuntimeCrdtClient<K, S> localClient, CrdtDescriptor<K, S> descriptor) {
		return CrdtClusterClient.create(
				localClient.getEventloop(),
				singletonMap(config.get("crdt.cluster.localPartitionId"), localClient),
				descriptor.getCombiner()
		).initialize(ofCrdtCluster(config.getChild("crdt.cluster"), localClient, descriptor));
	}

	@Provides
	@Singleton
	CrdtRepartitionController<String, K, S> provideCrdtRepartitionController(CrdtClusterClient<String, K, S> clusterClient, Config config) {
		return CrdtRepartitionController.create(clusterClient, config.get("crdt.cluster.localPartitionId"));
	}

	@Provides
	@Singleton
	CrdtServer<K, S> provideCrdtServer(Eventloop eventloop, RuntimeCrdtClient<K, S> client, CrdtDescriptor<K, S> descriptor, Config config) {
		return CrdtServer.create(eventloop, client, descriptor.getSerializer())
				.initialize(ofAbstractServer(config.getChild("crdt.server")));
	}

	@Provides
	@Cluster
	@Singleton
	CrdtServer<K, S> provideClusterServer(Eventloop eventloop, CrdtClusterClient<String, K, S> client, CrdtDescriptor<K, S> descriptor, Config config) {
		return CrdtServer.create(eventloop, client, descriptor.getSerializer())
				.initialize(ofAbstractServer(config.getChild("crdt.cluster.server")));
	}

	@Provides
	@Singleton
	Eventloop provideEventloop(Config config) {
		return Eventloop.create()
				.initialize(ofEventloop(config));
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
	@BindingAnnotation
	public @interface InMemory {}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
	@BindingAnnotation
	public @interface Persistent {}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
	@BindingAnnotation
	public @interface Cluster {}
}
