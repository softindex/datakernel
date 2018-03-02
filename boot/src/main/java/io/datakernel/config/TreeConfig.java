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

package io.datakernel.config;

import io.datakernel.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.datakernel.util.Preconditions.checkNotNull;

final class TreeConfig extends AbstractConfig {
	private static final Logger logger = LoggerFactory.getLogger(TreeConfig.class);

	private final TreeConfig root;
	private final Map<String, TreeConfig> children;
	private String value;

	// creators
	private TreeConfig(@Nullable TreeConfig root, Map<String, TreeConfig> children, @Nullable String value) {
		this.root = root;
		this.children = children;
		this.value = value;
	}

	public TreeConfig() {
		this(null, new LinkedHashMap<>(), null);
	}

	public void add(String path, String value) {
		if (THIS.equals(path)) {
			this.value = value;
			return;
		}
		int dot = path.lastIndexOf(DELIMITER);
		if (dot == -1) {
			this.addLeaf(path, value);
		} else {
			String pathToValue = path.substring(0, dot);
			String key = path.substring(dot + 1, path.length());
			addBranch(pathToValue).addLeaf(key, value);
		}
	}

	public TreeConfig addBranch(String path) {
		int dot = path.indexOf(DELIMITER);
		if (dot == -1) {
			return ensureBranch(path);
		} else {
			String childKey = path.substring(0, dot);
			String relativePath = path.substring(dot + 1, path.length());
			TreeConfig child = ensureBranch(childKey);
			return child.addBranch(relativePath);
		}
	}

	public void addLeaf(String key, String value) {
		assert this.value == null;
		assert !key.contains(DELIMITER);
		this.children.put(key, new TreeConfig(this, Collections.emptyMap(), checkNotNull(value)));
	}

	// api
	@Override
	public Set<String> getChildren() {
		return children.keySet();
	}

	@Override
	public boolean hasValue() {
		return value != null;
	}

	@Override
	public boolean doHasChild(String key) {
		return children.containsKey(key);
	}

	@Override
	protected String doGet() {
		if (value == null) {
			throw new NoSuchElementException(root == null ? "" : root.getKey(this));
		}
		return value;
	}

	@Override
	protected String doGet(String defaultString) {
		if (value == null) {
			logger.warn("using default config value {} for \"{}\"", defaultString, root == null ? "" : root.getKey(this));
			return defaultString;
		}
		return value;
	}

	@Override
	protected Config doGetChild(String key) {
		TreeConfig config = children.get(key);
		if (config == null) {
			return Config.EMPTY;
		}
		return config;
	}

	// config specific
	private String getKey(TreeConfig config) {
		for (Map.Entry<String, TreeConfig> entry : children.entrySet()) {
			TreeConfig value = entry.getValue();
			if (value == config) {
				return (root == null ? "" : root.getKey(this) + DELIMITER) + entry.getKey();
			}
		}
		throw new IllegalStateException();
	}

	private TreeConfig ensureBranch(String path) {
		if (path.isEmpty()) {
			throw new IllegalArgumentException("Path can not be empty");
		}
		TreeConfig child = this.children.get(path);
		if (child == null) {
			child = new TreeConfig(this, new LinkedHashMap<>(), null);
			this.children.put(path, child);
		} else {
			if (child.hasValue()) {
				throw new IllegalStateException("Branch node can't have values: " + getKey(this));
			}
		}
		return child;
	}

	@Override
	public boolean isEmpty() {
		return value == null && children.isEmpty();
	}

	@Override
	public String toString() {
		return "TreeConfig:[" + (value != null ? value : children.toString()) + "]";
	}
}
