package io.datakernel.docs.plugin.text;

import java.util.List;

/**
 * Syntax example {% link cloud-lsmt-aggregation/src/main/java/io/datakernel/aggregation/annotation/Key.java method=run } Title {% endlink %}
 */
public final class LinkTextPlugin implements TextPlugin {
	private static final String TAG = "link";
	private static final String LINK_TAG = "<a href=\"%s/%s\" target=\"_blank\">%s</a>";
	private final String githubUrl;

	public LinkTextPlugin(String githubUrl) {
		this.githubUrl = githubUrl;
	}

	@Override
	public String apply(String innerContent, List<String> params) throws PluginApplyException {
		if (params.size() < 1) {
			throw new PluginApplyException("Not enough params");
		}
		String className = params.get(0);
		return String.format(LINK_TAG, githubUrl, className, innerContent);
	}

	@Override
	public String getName() {
		return TAG;
	}
}
