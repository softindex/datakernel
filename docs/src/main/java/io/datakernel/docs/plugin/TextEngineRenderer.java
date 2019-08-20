package io.datakernel.docs.plugin;


import io.datakernel.docs.plugin.text.PluginApplyException;
import io.datakernel.docs.plugin.text.TextPlugin;
import io.datakernel.docs.render.ContentRenderException;
import io.datakernel.util.ref.RefInt;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;
import static java.util.regex.Pattern.DOTALL;

public class TextEngineRenderer {
	public static final String PROPERTIES = "properties";
	private static final String PATTERN = "\\{(\\{|%\\s*(?<tag>[\\w-]+))(?<params>.*?)\\s*(%|})}((?<inner>.*?)\\{%\\s*end\\2\\s*%})?";
	private static final String PARAMS_PATTERN = "\\s*(\".*?\"|[\\d\\w/.-]+|'.*'|`.*`)\\s*";
	private static final String EMPTY = "";
	private static final TextPlugin EMPTY_TAG_PLUGIN = new TextPlugin() {
		@Override
		public String apply(String innerContent, List<String> params) { return innerContent; }
		@Override
		public String getName() { return null; }
	};

	private final Map<String, TextPlugin> tagPlugins = new HashMap<>();
	private final Pattern paramsPattern = Pattern.compile(PARAMS_PATTERN, DOTALL);
	private final Pattern pattern = Pattern.compile(PATTERN, DOTALL);
	private TextPlugin plainTextPlugin = EMPTY_TAG_PLUGIN;

	private TextEngineRenderer() {}

	public static TextEngineRenderer create() {
		return new TextEngineRenderer();
	}

	public TextEngineRenderer withTextPlugins(Collection<TextPlugin> tags) {
		tags.forEach(plugin -> tagPlugins.put(plugin.getName(), plugin));
		return this;
	}

	public TextEngineRenderer withPlainTextPlugin(TextPlugin textPlugin) {
		this.plainTextPlugin = textPlugin;
		return this;
	}

	public String render(String content) throws ContentRenderException {
		StringBuilder renderedResult = new StringBuilder();
		RefInt previousEnd = new RefInt(0);
		Matcher match = pattern.matcher(content);
		ContentRenderException possibleException = new ContentRenderException();
		while (match.find()) {
			possibleException.wrapException(
					() -> renderedResult.append(plainTextPlugin.apply(content.substring(previousEnd.get(), match.start()), emptyList())));
			possibleException.wrapException(() -> {
                        renderedResult.append(process(content.substring(match.start(), match.end())));
                        previousEnd.set(match.end());
					});
		}
		possibleException.wrapException(() -> renderedResult.append(plainTextPlugin.apply(content.substring(previousEnd.get()), emptyList())));
		if (possibleException.hasReasonToThrow()) {
			throw possibleException;
		}
		return renderedResult.toString();
	}

	protected String process(String tagContent) throws PluginApplyException {
		Matcher match = pattern.matcher(tagContent);
		String tag = EMPTY;
		String params = EMPTY;
		String inner = tagContent;
		boolean isFound = match.find();
		if (isFound) {
			tag = (tag = match.group("tag")) != null ? tag : PROPERTIES;
			inner = match.group("inner");
			params = match.group("params");
		}
		TextPlugin tagPlugin = tagPlugins.getOrDefault(tag, EMPTY_TAG_PLUGIN);
		if (tagPlugin == EMPTY_TAG_PLUGIN && !tag.equals(EMPTY)) {
			throw new PluginApplyException("Tag '" + tag + "' is not found");
		}
		List<String> parsedParams = parseParams(params);
		return inner == null ?
				tagContent.replace(match.group(), tagPlugin.apply(EMPTY, parsedParams)) :
				tagPlugin.apply(isFound ? process(inner) : inner, parsedParams);
	}

	private List<String> parseParams(String params) {
		Matcher match = paramsPattern.matcher(params);
		ArrayList<String> paramList = new ArrayList<>();
		while (match.find()) {
			paramList.add(match.group(1));
		}
		return paramList;
	}
}
