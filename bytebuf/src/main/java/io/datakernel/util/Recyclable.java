package io.datakernel.util;

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
				Object next = it.next();
				deepRecycle(it.next());
			}
		} else if (object instanceof Iterable) {
			deepRecycle(object);
		} else if (object instanceof Map) {
			deepRecycle(((Map) object).values());
		} else if (object instanceof Object[]) {
			for (Object element : (Object[]) object) {
				deepRecycle(element);
			}
		}
	}
}
