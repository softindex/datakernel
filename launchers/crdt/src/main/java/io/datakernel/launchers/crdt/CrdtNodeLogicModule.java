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

import io.datakernel.common.reflection.RecursiveType;
import io.datakernel.config.Config;
import io.datakernel.crdt.CrdtRepartitionController;
import io.datakernel.crdt.CrdtServer;
import io.datakernel.crdt.CrdtStorage;
import io.datakernel.crdt.CrdtStorageCluster;
import io.datakernel.crdt.local.CrdtStorageFs;
import io.datakernel.crdt.local.CrdtStorageMap;
import io.datakernel.di.Key;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.QualifierAnnotation;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.FsClient;
import org.jetbrains.annotations.NotNull;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.launchers.crdt.Initializers.ofCrdtCluster;
import static io.datakernel.launchers.crdt.Initializers.ofFsCrdtClient;
import static io.datakernel.launchers.initializers.Initializers.ofAbstractServer;
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

public abstract class CrdtNodeLogicModule<K extends Comparable<K>, S> extends AbstractModule {
	@SuppressWarnings("unchecked")
	@Override
	protected void configure() {
		Type genericSuperclass = getClass().getGenericSuperclass();
		Type[] typeArguments = ((ParameterizedType) genericSuperclass).getActualTypeArguments();

		List<RecursiveType> typeArgs = Arrays.stream(typeArguments).map(RecursiveType::of).collect(toList());
		@NotNull Type supertype = RecursiveType.of(CrdtStorage.class, typeArgs).getType();

		bind((Key<?>) Key.ofType(supertype, InMemory.class))
				.to(Key.ofType(RecursiveType.of(CrdtStorageMap.class, typeArgs).getType()));
		bind((Key<?>) Key.ofType(supertype, Persistent.class))
				.to(Key.ofType(RecursiveType.of(CrdtStorageFs.class, typeArgs).getType()));

		typeArgs.add(0, RecursiveType.of(String.class));
		bind((Key<?>) Key.ofType(supertype, Cluster.class))
				.to(Key.ofType(RecursiveType.of(CrdtStorageCluster.class, typeArgs).getType()));
	}

	@Provides
	CrdtStorageMap<K, S> runtimeCrdtClient(Eventloop eventloop, CrdtDescriptor<K, S> descriptor) {
		return CrdtStorageMap.create(eventloop, descriptor.getCrdtFunction());
	}

	@Provides
	CrdtStorageFs<K, S> fsCrdtClient(Eventloop eventloop, Config config, FsClient fsClient, CrdtDescriptor<K, S> descriptor) {
		return CrdtStorageFs.create(eventloop, fsClient, descriptor.getSerializer(), descriptor.getCrdtFunction())
				.initialize(ofFsCrdtClient(config));
	}

	@Provides
	CrdtStorageCluster<String, K, S> clusterCrdtClient(Config config, CrdtStorageMap<K, S> localClient, CrdtDescriptor<K, S> descriptor) {
		return CrdtStorageCluster.create(
				localClient.getEventloop(),
				singletonMap(config.get("crdt.cluster.localPartitionId"), localClient),
				descriptor.getCrdtFunction())
				.initialize(ofCrdtCluster(config.getChild("crdt.cluster"), localClient, descriptor));
	}

	@Provides
	CrdtRepartitionController<String, K, S> crdtRepartitionController(CrdtStorageCluster<String, K, S> clusterClient, Config config) {
		return CrdtRepartitionController.create(clusterClient, config.get("crdt.cluster.localPartitionId"));
	}

	@Provides
	CrdtServer<K, S> crdtServer(Eventloop eventloop, CrdtStorageMap<K, S> client, CrdtDescriptor<K, S> descriptor, Config config) {
		return CrdtServer.create(eventloop, client, descriptor.getSerializer())
				.initialize(ofAbstractServer(config.getChild("crdt.server")));
	}

	@Provides
	@Cluster
	CrdtServer<K, S> clusterServer(Eventloop eventloop, CrdtStorageCluster<String, K, S> client, CrdtDescriptor<K, S> descriptor, Config config) {
		return CrdtServer.create(eventloop, client, descriptor.getSerializer())
				.initialize(ofAbstractServer(config.getChild("crdt.cluster.server")));
	}

	@Provides
	Eventloop eventloop(Config config) {
		return Eventloop.create()
				.initialize(ofEventloop(config));
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
	@QualifierAnnotation
	public @interface InMemory {}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
	@QualifierAnnotation
	public @interface Persistent {}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
	@QualifierAnnotation
	public @interface Cluster {}
}
