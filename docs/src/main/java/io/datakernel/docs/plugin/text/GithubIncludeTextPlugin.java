package io.datakernel.docs.plugin.text;

import io.datakernel.docs.dao.ResourceDao;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.regex.Pattern.DOTALL;
import static java.util.regex.Pattern.MULTILINE;

public class GithubIncludeTextPlugin implements TextPlugin {
	public static final String TAG = "github_sample";
	public static final String EMPTY = "";
	public static final String PREFIX = "softindex/datakernel/blob/master/";
	public static final String START_END_TAG = "\\//\\s*\\[START\\s+%1$s]\\n((\\t*).+?)\\s*//\\s*\\[END\\s+%1$s\\]";

	protected final ResourceDao resourceDao;

	public GithubIncludeTextPlugin(ResourceDao resourceDao) {
		this.resourceDao = resourceDao;
	}

	@Override
	public String apply(String innerContent, List<String> params) throws PluginApplyException {
		if (params.size() < 1) {
			throw new PluginApplyException("Not enough params");
		}
		String resourceName = params.get(0);
			try {
			String content = resourceDao.getResource(resourceName.replace(PREFIX, EMPTY));
			if (params.size() < 2) {
				return content;
			}
			String tag = params.get(2);
			return findByTag(content, tag);
		} catch (IOException e) {
			throw new PluginApplyException(e);
		}
	}

	protected String findByTag(String content, String tag) throws PluginApplyException {
		Matcher startEndContentMatch = Pattern.compile(format(START_END_TAG, tag), DOTALL)
				.matcher(content);
		if (startEndContentMatch.find()) {
			return Pattern.compile('^' + startEndContentMatch.group(2), MULTILINE)
					.matcher(startEndContentMatch.group(1))
					.replaceAll(EMPTY);
		} else {
			throw new PluginApplyException("Cannot find tag '" + tag + "'");
		}
	}

	@Override
	public String getName() {
		return TAG;
	}
}
