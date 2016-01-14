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

package io.datakernel.boot;

import com.google.common.reflect.TypeToken;

import java.util.List;

public interface WorkerThreadsPool {
	String getPoolName();

	int getPoolSize();

	<T> List<T> getPoolInstances(Class<T> type);

	<T> List<T> getPoolInstances(TypeToken<T> type);

	<T> List<T> getPoolInstances(Class<T> type, String instanceName);

	<T> List<T> getPoolInstances(TypeToken<T> type, String instanceName);
}
