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
import java.util.List;
import java.util.Map;
import java.util.Set;

interface AttributeNode {
	String getName();

	OpenType<?> getOpenType();

	Map<String, OpenType<?>> getVisibleFlattenedOpenTypes();

	Set<String> getAllFlattenedAttrNames();

	Map<String, Map<String, String>> getDescriptions();

	Map<String, Object> aggregateAllAttributes(List<?> sources);

	Object aggregateAttribute(String attrName, List<?> sources);

	Iterable<JmxRefreshable> getAllRefreshables(Object source);

	boolean isSettable(String attrName);

	void setAttribute(String attrName, Object value, List<?> targets) throws SetterException;

	// TODO(vmykhalko): rename into "transformOmittingNullPojos" ?
	// TODO(vmykhalko): maybe make such nodes not visible but not to remove
	AttributeNode rebuildOmittingNullPojos(List<?> sources);

	boolean isVisible();

	// TODO(vmykhalko): maybe make tree mutable for simplification ?
	AttributeNode rebuildWithVisible(String attrName);

//	AttributeNode exclude(String attrName);

//	boolean applyModifier(String attrName, Modifier<?> modifier);
}
