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

package io.global.ot.demo.client;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTRepository;
import io.global.ot.api.CommitId;
import io.global.ot.demo.operations.Operation;

import java.util.concurrent.ExecutorService;

import static io.global.ot.demo.util.Utils.createOTSystem;
import static java.util.concurrent.Executors.newCachedThreadPool;

final class OTStateModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(ExecutorService.class).toInstance(newCachedThreadPool());
	}

	@Provides
	@Singleton
	OTAlgorithms<CommitId, Operation> provide(Eventloop eventloop, OTRepository<CommitId, Operation> repository) {
		return OTAlgorithms.create(eventloop, createOTSystem(), repository);
	}

}
