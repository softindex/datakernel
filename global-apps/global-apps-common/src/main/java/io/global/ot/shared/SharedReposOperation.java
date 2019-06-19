package io.global.ot.shared;

import java.util.Map;

public interface SharedReposOperation {
	void apply(Map<String, SharedRepo> repos);
}
