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

import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import java.util.*;

import static io.datakernel.jmx.Utils.filterNulls;
import static io.datakernel.util.Preconditions.*;

abstract class AttributeNodeForPojoAbstract implements AttributeNode {
	private static final String COMPOSITE_TYPE_DEFAULT_NAME = "CompositeType";
	protected static final String ATTRIBUTE_NAME_SEPARATOR = "_";

	protected final String name;
	protected final String description;
	protected final boolean visible;
	protected final ValueFetcher fetcher;
	private final OpenType<?> openType;
	protected final Map<String, OpenType<?>> visibleNameToOpenType;
	protected final Map<String, AttributeNode> fullNameToNode;
	private final List<? extends AttributeNode> subNodes;

	public AttributeNodeForPojoAbstract(String name, String description, boolean visible,
	                                    ValueFetcher fetcher, List<? extends AttributeNode> subNodes) {
		this.name = name;
		this.description = description;
		this.visible = visible;
		this.fetcher = fetcher;
		this.openType = createOpenType(name, subNodes);
		this.visibleNameToOpenType = createNameToOpenTypeMap(name, subNodes);
		this.fullNameToNode = createFullNameToNodeMapping(name, subNodes);
		this.subNodes = subNodes;
	}

	private static Map<String, AttributeNode> createFullNameToNodeMapping(String name,
	                                                                      List<? extends AttributeNode> subNodes) {
		Map<String, AttributeNode> fullNameToNodeMapping = new HashMap<>();
		String prefix = name.isEmpty() ? "" : name + ATTRIBUTE_NAME_SEPARATOR;
		for (AttributeNode subNode : subNodes) {
			Set<String> currentSubAttrNames = subNode.getAllFlattenedAttrNames();
			for (String currentSubAttrName : currentSubAttrNames) {
				String currentAttrFullName = prefix + currentSubAttrName;
				if (fullNameToNodeMapping.containsKey(currentAttrFullName)) {
					throw new IllegalArgumentException(
							"There are several attributes with same name: " + currentSubAttrName);
				}
				fullNameToNodeMapping.put(currentAttrFullName, subNode);
			}
		}
		return fullNameToNodeMapping;
	}

	private static Map<String, OpenType<?>> createNameToOpenTypeMap(String nodeName,
	                                                                List<? extends AttributeNode> subNodes) {
		Map<String, OpenType<?>> nameToOpenType = new HashMap<>();
		String prefix = nodeName.isEmpty() ? "" : nodeName + ATTRIBUTE_NAME_SEPARATOR;
		for (AttributeNode subNode : subNodes) {
//			if (subNode.isVisible()) {
			Map<String, OpenType<?>> currentSubNodeMap = subNode.getVisibleFlattenedOpenTypes();
			for (String subNodeAttrName : currentSubNodeMap.keySet()) {
				nameToOpenType.put(prefix + subNodeAttrName, currentSubNodeMap.get(subNodeAttrName));
			}
//			}
		}
		return nameToOpenType;
	}

	private static OpenType createOpenType(String name, List<? extends AttributeNode> subNodes) {
		if (subNodes.size() == 0) {
			// As far as MBean is itself a POJO and could potentially contain only operation (and no attributes),
			// we can't use preconditions like this: [checkArgument(subNodes.size() > 0)
			// CompositeType cannot be empty, so there's no other obvious options than returning null
			return null;
		}

		//			 TODO(vmykhalko): refactor
		Map<String, OpenType<?>> allVisibleAttrs = new HashMap<>();
		for (AttributeNode subNode : subNodes) {
			allVisibleAttrs.putAll(subNode.getVisibleFlattenedOpenTypes());
		}
		if (allVisibleAttrs.size() == 1 && allVisibleAttrs.containsKey("")) {
			return allVisibleAttrs.values().iterator().next();
		}

		List<String> itemNames = new ArrayList<>();
		List<OpenType<?>> itemTypes = new ArrayList<>();
		String prefix = name.isEmpty() ? "" : name + ATTRIBUTE_NAME_SEPARATOR;
		for (AttributeNode subNode : subNodes) {
			Map<String, OpenType<?>> subNodeFlattenedTypes = subNode.getVisibleFlattenedOpenTypes();
			for (String attrName : subNodeFlattenedTypes.keySet()) {
				itemNames.add(prefix + attrName);
				itemTypes.add(subNodeFlattenedTypes.get(attrName));
			}
		}
		String[] itemNamesArr = itemNames.toArray(new String[itemNames.size()]);
		OpenType<?>[] itemTypesArr = itemTypes.toArray(new OpenType<?>[itemTypes.size()]);
		try {
			return new CompositeType(
					COMPOSITE_TYPE_DEFAULT_NAME, COMPOSITE_TYPE_DEFAULT_NAME,
					itemNamesArr, itemNamesArr,
					itemTypesArr);
		} catch (OpenDataException e) {
			throw new RuntimeException(e);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected final List<Object> fetchInnerPojos(List<?> outerPojos) {
		List<Object> innerPojos = new ArrayList<>(outerPojos.size());
		for (Object outerPojo : outerPojos) {
			Object pojo = fetcher.fetchFrom(outerPojo);
			if (pojo != null) {
				innerPojos.add(pojo);
			}
		}
		return innerPojos;
	}

	@Override
	public final String getName() {
		return name;
	}

	@Override
	public Map<String, Map<String, String>> getDescriptions() {
		Map<String, Map<String, String>> nameToDescriptions = new HashMap<>();
		String prefix = name.isEmpty() ? "" : name + ATTRIBUTE_NAME_SEPARATOR;
		for (AttributeNode subNode : subNodes) {
			Map<String, Map<String, String>> currentSubNodeDescriptions = subNode.getDescriptions();
			for (String subNodeAttrName : currentSubNodeDescriptions.keySet()) {
				String resultAttrName = prefix + subNodeAttrName;
				Map<String, String> curDescriptions = new LinkedHashMap<>();
				if (description != null) {
					curDescriptions.put(name, description);
				}
				curDescriptions.putAll(currentSubNodeDescriptions.get(subNodeAttrName));
				nameToDescriptions.put(resultAttrName, curDescriptions);
			}
		}
		return nameToDescriptions;
	}

	@Override
	public final OpenType<?> getOpenType() {
		return openType;
	}

	@Override
	public final Map<String, OpenType<?>> getVisibleFlattenedOpenTypes() {
		return visibleNameToOpenType;
	}

	@Override
	public Set<String> getAllFlattenedAttrNames() {
		return fullNameToNode.keySet();
	}

	@Override
	public final boolean isSettable(String attrName) {
		if (!fullNameToNode.containsKey(attrName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrName);
		}

		AttributeNode appropriateSubNode = fullNameToNode.get(attrName);

		String subAttrName;
		if (name.isEmpty()) {
			subAttrName = attrName;
		} else {
			checkArgument(attrName.contains(ATTRIBUTE_NAME_SEPARATOR));
			int indexOfSeparator = attrName.indexOf(ATTRIBUTE_NAME_SEPARATOR);
			subAttrName = attrName.substring(indexOfSeparator + 1, attrName.length());
		}

		return appropriateSubNode.isSettable(subAttrName);
	}

	@Override
	public final void setAttribute(String attrName, Object value, List<?> targets) throws SetterException {
		checkNotNull(targets);
		List<?> notNullTargets = filterNulls(targets);
		if (notNullTargets.size() == 0) {
			return;
		}

		if (!fullNameToNode.containsKey(attrName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrName);
		}

		AttributeNode appropriateSubNode = fullNameToNode.get(attrName);

		if (name.isEmpty()) {
			appropriateSubNode.setAttribute(attrName, value, fetchInnerPojos(targets));
		} else {
			checkArgument(attrName.contains(ATTRIBUTE_NAME_SEPARATOR));
			int indexOfSeparator = attrName.indexOf(ATTRIBUTE_NAME_SEPARATOR);
			String subAttrName = attrName.substring(indexOfSeparator + 1, attrName.length());
			appropriateSubNode.setAttribute(subAttrName, value, fetchInnerPojos(targets));
		}
	}

	@Override
	public AttributeNode rebuildOmittingNullPojos(List<?> sources) {
		List<?> innerPojos = fetchInnerPojos(sources);
		if (innerPojos.size() == 0) {
			return null;
		}

		List<AttributeNode> filteredNodes = new ArrayList<>();
		for (AttributeNode subNode : subNodes) {
			AttributeNode rebuilded = subNode.rebuildOmittingNullPojos(innerPojos);
			if (rebuilded != null) {
				filteredNodes.add(rebuilded);
			}
		}

		return filteredNodes.size() > 0 ? recreate(filteredNodes, visible) : null;
	}

	protected abstract AttributeNode recreate(List<? extends AttributeNode> filteredNodes, boolean visible);

	@Override
	public boolean isVisible() {
		return visible;
	}

	@Override
	public AttributeNode rebuildWithVisible(String attrName) {
		if (attrName.equals(name)) {
			return recreate(subNodes, true);
		}

		// TODO(vmykhalko): refactor (extract common code)
		String subAttrName;
		if (name.isEmpty()) {
			subAttrName = attrName;
		} else {
			checkArgument(attrName.contains(ATTRIBUTE_NAME_SEPARATOR));
			int indexOfSeparator = attrName.indexOf(ATTRIBUTE_NAME_SEPARATOR);
			subAttrName = attrName.substring(indexOfSeparator + 1, attrName.length());
		}

		List<AttributeNode> rebuildedSubnodes = new ArrayList<>();
		boolean subNodeFound = false;
		for (AttributeNode subNode : subNodes) {
			Set<String> subAttrs = subNode.getAllFlattenedAttrNames();
			AttributeNode actualSubNode = subNode;
			for (String subAttr : subAttrs) {
				// TODO(vmykhalko): refactor
				if (subAttr.startsWith(subAttrName) &&
						(subAttr.length() == subAttrName.length() ||
								subAttr.substring(subAttrName.length()).startsWith(ATTRIBUTE_NAME_SEPARATOR))) {
					checkState(!subNodeFound);
					subNodeFound = true;
					actualSubNode = subNode.rebuildWithVisible(subAttrName);
					break;
				}
			}
			rebuildedSubnodes.add(actualSubNode);
		}

		return recreate(rebuildedSubnodes, true);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void applyModifier(String attrName, AttributeModifier<?> modifier, List<?> target) {
		if (attrName.equals(name)) {
			AttributeModifier attrModifierRaw = modifier;
			List<Object> attributes = fetchInnerPojos(target);
			for (Object attribute : attributes) {
				attrModifierRaw.apply(attribute);
			}
			return;
		}

		String subAttrName;
		if (name.isEmpty()) {
			subAttrName = attrName;
		} else {
			checkArgument(attrName.contains(ATTRIBUTE_NAME_SEPARATOR));
			int indexOfSeparator = attrName.indexOf(ATTRIBUTE_NAME_SEPARATOR);
			subAttrName = attrName.substring(indexOfSeparator + 1, attrName.length());
		}

		for (AttributeNode subNode : subNodes) {
			Set<String> subAttrs = subNode.getAllFlattenedAttrNames();
			for (String subAttr : subAttrs) {
				// TODO(vmykhalko): refactor
				if (subAttr.startsWith(subAttrName) &&
						(subAttr.length() == subAttrName.length() ||
								subAttr.substring(subAttrName.length()).startsWith(ATTRIBUTE_NAME_SEPARATOR))) {
					subNode.applyModifier(subAttrName, modifier, fetchInnerPojos(target));
					return;
				}
			}
		}

		throw new RuntimeException("Cannot apply modifier. Attribute not found: " + attrName);
	}
}
