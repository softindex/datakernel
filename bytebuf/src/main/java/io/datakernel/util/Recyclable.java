package io.datakernel.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public interface Recyclable {
	void recycle();

	static void deepRecycle(Object object) {
		if (object == null) return;
		if (object instanceof Recyclable) {
			Recyclable recyclable = (Recyclable) object;
			recyclable.recycle();
		} else if (object instanceof Iterator) {
			Iterator it = (Iterator) object;
			while (it.hasNext()) {
				deepRecycle(it.next());
			}
		} else if (object instanceof Collection) {
			deepRecycle(object);
			((Collection) object).clear();
		} else if (object instanceof Iterable) {
			deepRecycle(object);
		} else if (object instanceof Map) {
			deepRecycle(((Map) object).values());
			((Map) object).clear();
		} else if (object instanceof Object[]) {
			Object[] objects = (Object[]) object;
			for (Object element : objects) {
				deepRecycle(element);
			}
			Arrays.fill(objects, null);
		}
	}
}
