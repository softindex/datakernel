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

package io.datakernel.util;

import io.datakernel.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public interface Recyclable {
	void recycle();

	static void tryRecycle(@Nullable Object object) {
		if (object instanceof Recyclable) {
			((Recyclable) object).recycle();
		}
	}

	static void deepRecycle(@Nullable Object object) {
		if (object == null) return;
		if (object instanceof Recyclable) {
			Recyclable recyclable = (Recyclable) object;
			recyclable.recycle();
		} else if (object instanceof Iterator) {
			Iterator<?> it = (Iterator<?>) object;
			while (it.hasNext()) {
				deepRecycle(it.next());
			}
		} else if (object instanceof Collection) {
			deepRecycle(((Collection<?>) object).iterator());
			((Collection<?>) object).clear();
		} else if (object instanceof Iterable) {
			deepRecycle(((Iterable<?>) object).iterator());
		} else if (object instanceof Map) {
			deepRecycle(((Map<?, ?>) object).values());
			((Map<?, ?>) object).clear();
		} else if (object instanceof Object[]) {
			Object[] objects = (Object[]) object;
			for (Object element : objects) {
				deepRecycle(element);
			}
			Arrays.fill(objects, null);
		}
	}
}
