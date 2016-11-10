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

package io.datakernel.cube.attributes;

import io.datakernel.async.CompletionCallback;

import java.util.List;

public interface AttributeResolver {
	Class<?>[] getKeyTypes();

	Class<?>[] getAttributeTypes();

	void resolveAttributes(List<Object> results, KeyFunction keyFunction, AttributesFunction attributesFunction,
	                       CompletionCallback callback);

	interface KeyFunction {
		Object[] extractKey(Object result);
	}

	interface AttributesFunction {
		void applyAttributes(Object result, Object[] attributes);
	}
}
