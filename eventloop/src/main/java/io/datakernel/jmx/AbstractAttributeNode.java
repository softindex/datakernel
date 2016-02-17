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

package io.datakernel.jmx;

import javax.management.openmbean.OpenType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

abstract class AbstractAttributeNode implements AttributeNode {
	private final String name;
	private final Method getter;
	private final OpenType<?> openType;

	public AbstractAttributeNode(String name, Method getter, OpenType<?> openType) {
		this.name = checkNotNull(name);
		this.getter = checkNotNull(getter);
		this.openType = checkNotNull(openType);
	}

	@Override
	public final String getName() {
		return name;
	}

	@Override
	public final OpenType<?> getOpenType() {
		return openType;
	}

	protected Object fetchValueFrom(Object pojo) {
		try {
			return getter.invoke(pojo);
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Throws Exception If list is null, empty or at least one element of list is null
	 */
	protected static void checkPojos(List<?> pojos) {
		checkNotNull(pojos);
		checkArgument(pojos.size() > 0);
		for (Object pojo : pojos) {
			checkNotNull(pojo);
		}
	}
}
