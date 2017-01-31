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

package io.datakernel.jmx.helper;

import javax.management.MBeanAttributeInfo;
import java.util.HashMap;
import java.util.Map;

public class Utils {
	public static Map<String, MBeanAttributeInfo> nameToAttribute(MBeanAttributeInfo[] attrs) {
		Map<String, MBeanAttributeInfo> nameToAttr = new HashMap<>();
		for (MBeanAttributeInfo attr : attrs) {
			nameToAttr.put(attr.getName(), attr);
		}
		return nameToAttr;
	}
}
