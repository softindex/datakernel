package io.datakernel.docs.plugin.text;

import io.datakernel.docs.dao.ResourceDao;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.regex.Pattern.MULTILINE;

public final class GithubIncludeTextPlugin implements TextPlugin {
	private static final String TAG = "github_sample";
	private static final String EMPTY = "";
	private static final String PREFIX = "softindex/datakernel/blob/master/";
	private static final String START_END_TAG = "\\//\\s*\\[START\\s+%1$s]\\n((\\t*).+?)\\s*//\\s*\\[END\\s+%1$s\\]";

	private final ResourceDao resourceDao;

	public GithubIncludeTextPlugin(ResourceDao resourceDao) {
		this.resourceDao = resourceDao;
	}

	@Override
	public String apply(String innerContent, List<String> params) throws PluginApplyException {
		String resourceName = params.get(0);
			try {
			String content = resourceDao.getResource(resourceName.replace(PREFIX, EMPTY));
			if (params.size() < 2) {
				return content;
			}
			String tag = params.get(2);
			Matcher startEndContentMatch = Pattern.compile(format(START_END_TAG, tag), Pattern.DOTALL)
					.matcher(content);
			if (startEndContentMatch.find()) {
				return Pattern.compile('^' + startEndContentMatch.group(2), MULTILINE)
						.matcher(startEndContentMatch.group(1))
						.replaceAll(EMPTY);
			} else {
				throw new PluginApplyException("Cannot find tag '" + tag + "'");
			}
		} catch (IOException e) {
			throw new PluginApplyException(e);
		}
	}

	@Override
	public String getName() {
		return TAG;
	}
}
