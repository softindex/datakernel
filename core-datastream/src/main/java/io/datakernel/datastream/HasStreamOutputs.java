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

package io.datakernel.datastream;

import java.util.List;

/**
 * A separate interface for components that have multiple outputs represented with a list of {@link StreamSupplier consumers}.
 */
public interface HasStreamOutputs<O> {
	List<? extends StreamSupplier<O>> getOutputs();

	default StreamSupplier<O> getOutput(int index) {
		return getOutputs().get(index);
	}
}