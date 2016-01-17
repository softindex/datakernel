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

package io.datakernel.worker;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class WorkerPoolObjects {
	final WorkerPool workerPool;
	final Object[] objects;

	public WorkerPoolObjects(WorkerPool workerPool, Object[] objects) {
		this.workerPool = workerPool;
		this.objects = objects;
	}

	public WorkerPool getWorkerPool() {
		return workerPool;
	}

	public List<Object> getObjects() {
		return Collections.unmodifiableList(Arrays.asList(objects));
	}
}
