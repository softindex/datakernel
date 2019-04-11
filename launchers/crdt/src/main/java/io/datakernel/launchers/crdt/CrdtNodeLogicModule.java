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
import com.google.inject.util.Types;
import io.datakernel.config.Config;
import io.datakernel.crdt.CrdtRepartitionController;
import io.datakernel.crdt.CrdtServer;
import io.datakernel.crdt.CrdtStorage;
import io.datakernel.crdt.CrdtStorageCluster;
import io.datakernel.crdt.local.CrdtStorageFs;
import io.datakernel.crdt.local.CrdtStorageMap;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.FsClient;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import static io.datakernel.launchers.crdt.Initializers.ofCrdtCluster;
import static io.datakernel.launchers.crdt.Initializers.ofFsCrdtClient;
import static io.datakernel.launchers.initializers.Initializers.ofAbstractServer;
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;
import static java.util.Collections.singletonMap;

public abstract class CrdtNodeLogicModule<K extends Comparable<K>, S> extends AbstractModule {

	@SuppressWarnings("unchecked")
	@Override
	protected void configure() {
		Type genericSuperclass = getClass().getGenericSuperclass();
		Type[] typeArguments = ((ParameterizedType) genericSuperclass).getActualTypeArguments();

		Type supertype = Types.newParameterizedType(CrdtStorage.class, typeArguments);

		bind((Key) Key.get(supertype, InMemory.class))
				.to(Key.get(Types.newParameterizedType(CrdtStorageMap.class, typeArguments)));
		bind((Key) Key.get(supertype, Persistent.class))
				.to(Key.get(Types.newParameterizedType(CrdtStorageFs.class, typeArguments)));
		bind((Key) Key.get(supertype, Cluster.class))
				.to(Key.get(Types.newParameterizedType(CrdtStorageCluster.class, String.class, typeArguments[0], typeArguments[1])));
	}

	@Provides
	@Singleton
	CrdtStorageMap<K, S> provideRuntimeCrdtClient(Eventloop eventloop, CrdtDescriptor<K, S> descriptor) {
		return CrdtStorageMap.create(eventloop, descriptor.getCrdtFunction());
	}

	@Provides
	@Singleton
	CrdtStorageFs<K, S> provideFsCrdtClient(Eventloop eventloop, Config config, FsClient fsClient, CrdtDescriptor<K, S> descriptor) {
		return CrdtStorageFs.create(eventloop, fsClient, descriptor.getSerializer(), descriptor.getCrdtFunction())
				.initialize(ofFsCrdtClient(config));
	}

	@Provides
	@Singleton
	CrdtStorageCluster<String, K, S> provideClusterCrdtClient(Config config, CrdtStorageMap<K, S> localClient, CrdtDescriptor<K, S> descriptor) {
		return CrdtStorageCluster.create(
				localClient.getEventloop(),
				singletonMap(config.get("crdt.cluster.localPartitionId"), localClient),
				descriptor.getCrdtFunction()
		).initialize(ofCrdtCluster(config.getChild("crdt.cluster"), localClient, descriptor));
	}

	@Provides
	@Singleton
	CrdtRepartitionController<String, K, S> provideCrdtRepartitionController(CrdtStorageCluster<String, K, S> clusterClient, Config config) {
		return CrdtRepartitionController.create(clusterClient, config.get("crdt.cluster.localPartitionId"));
	}

	@Provides
	@Singleton
	CrdtServer<K, S> provideCrdtServer(Eventloop eventloop, CrdtStorageMap<K, S> client, CrdtDescriptor<K, S> descriptor, Config config) {
		return CrdtServer.create(eventloop, client, descriptor.getSerializer())
				.initialize(ofAbstractServer(config.getChild("crdt.server")));
	}

	@Provides
	@Cluster
	@Singleton
	CrdtServer<K, S> provideClusterServer(Eventloop eventloop, CrdtStorageCluster<String, K, S> client, CrdtDescriptor<K, S> descriptor, Config config) {
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
