package io.datakernel.docs.plugin.text;

import io.datakernel.docs.dao.ResourceDao;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.DOTALL;

public final class HtmlIncludeTextPlugin implements TextPlugin {
	private static final String INCLUDE_CONTENT = "\\{\\{\\s*include[._](?<param>.*?)\\s*}}";
	private static final String TAG = "include";
	private static final String EMPTY = "";
	private final Pattern contentPattern = Pattern.compile(INCLUDE_CONTENT, DOTALL);
	private final ResourceDao resourceDao;

	public HtmlIncludeTextPlugin(ResourceDao resourceDao) {
		this.resourceDao = resourceDao;
	}

	private String getReplacedContent(String fileContent, Map<String, String> params) {
		StringBuilder content = new StringBuilder(fileContent);
		Matcher contentMatch = contentPattern.matcher(fileContent);
		int offset = 0;
		while (contentMatch.find()) {
			String key = contentMatch.group("param");
			String value = params.getOrDefault(key, EMPTY);
			content.replace(contentMatch.start() + offset, contentMatch.end() + offset, value);
			offset += value.length() - (contentMatch.end() - contentMatch.start());
		}
		return content.toString();
	}

	@Override
	public String apply(String innerContent, List<String> params) throws PluginApplyException {
		try {
			String filename = params.get(0);
			Map<String, String> injectParams = new HashMap<>();
			if (!(params.size() % 2 == 0)) {
				for (int i = 1; i < params.size(); i += 2) {
					injectParams.put(params.get(i), params.get(i + 1));
				}
			}
			return getReplacedContent(resourceDao.getResource(filename), injectParams);
		} catch (IOException e) {
			throw new PluginApplyException(e);
		}
	}

	@Override
	public String getName() {
		return TAG;
	}
}
