package io.datakernel.docs.plugin.text;

import io.datakernel.docs.dao.ResourceDao;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.regex.Pattern.DOTALL;
import static java.util.regex.Pattern.MULTILINE;

/**
 * Usage example: {% github_sample .../java/ServerSetupExample.java tag:START method=name class=name attribute=name }
 * Params are optional, for the one tag can be used only one option or none
 */
public final class GithubIncludeClassComponentsTextPlugin extends GithubIncludeTextPlugin {
	private static final String CLASS_REGEX = "^(\\t*)(public\\s+)?(final\\s+)?(static\\s+)?(final\\s+)?(abstract\\s+)?class\\s+%s(<.*>)?(.*?)\\s*\\{";
	private static final String METHOD_REGEX = "^(\\t+)[\\w<>\\s]+(%s\\([^;]*?\\))\\s*(throws\\s+.+?)?\\{";
	private static final String ATTRIBUTE_REGEX = "^(\\t+).*?\\w+(<.*>)?\\s+%s\\s*=?";
	private static final char OPEN_BRACKET = '{';
	private static final char SEMICOLON = ';';
	private static Map<Character, Character> mapSeparators = new HashMap<>();
	static {
		mapSeparators.put(';', ';');
		mapSeparators.put('\'', '\'');
		mapSeparators.put('"', '"');
		mapSeparators.put('(', ')');
		mapSeparators.put('{', '}');
	}
	private static final List<Character> openSeparators = asList('"', '\'', '(', '{');

	public GithubIncludeClassComponentsTextPlugin(ResourceDao resourceDao) {
		super(resourceDao);
	}

	@Override
	public String apply(String innerContent, List<String> params) throws PluginApplyException {
		if (params.size() < 1) {
			throw new PluginApplyException("Not enough params");
		}
		String resourceName = params.get(0);
		Map<String, String> paramsMap = new HashMap<>();
		for (int i = 1; i < params.size(); i += 2) {
			paramsMap.put(params.get(i), params.get(i + 1));
		}
		try {
			String content = resourceDao.getResource(resourceName.replace(PREFIX, EMPTY));
			if (paramsMap.isEmpty()) {
				return content;
			}
			String tagName = paramsMap.get("tag");
			if (tagName != null) {
				return findByTag(content, tagName);
			}
			String methodName = paramsMap.get("method");
			if (methodName != null) {
				return findByMethod(content, methodName);
			}
			String className = paramsMap.get("class");
			if (className != null) {
				return findClass(content, className);
			}
			String attributeName = paramsMap.get("attribute");
			if (attributeName != null) {
				return findAttributeName(content, attributeName);
			}
			return content;
		} catch (IOException e) {
			throw new PluginApplyException(e);
		}
	}

	private String findClass(String content, String className) throws PluginApplyException {
		Pattern pattern = Pattern.compile(String.format(CLASS_REGEX, className), DOTALL | MULTILINE);
		Matcher matcher = pattern.matcher(content);
		if (matcher.find()) {
			int start = matcher.start();
			int end = findEndSymbol(content.toCharArray(), matcher.end(), OPEN_BRACKET) + 1;
			return Pattern.compile('^' + matcher.group(1), MULTILINE)
					.matcher(content.substring(start, end))
					.replaceAll(EMPTY);
		} else {
			throw new PluginApplyException("Cannot find className '" + className + "'");
		}
	}

	private String findAttributeName(String content, String attributeName) throws PluginApplyException {
		Pattern pattern = Pattern.compile(String.format(ATTRIBUTE_REGEX, attributeName), MULTILINE);
		Matcher matcher = pattern.matcher(content);
		if (matcher.find()) {
			int start = matcher.start();
			int end = findEndSymbol(content.toCharArray(), matcher.end(), SEMICOLON) + 1;
			return Pattern.compile('^' + matcher.group(1), MULTILINE)
					.matcher(content.substring(start, end))
					.replaceAll(EMPTY);
		} else {
			throw new PluginApplyException("Cannot find attribute '" + attributeName + "'");
		}
	}

	private String findByMethod(String content, String method) throws PluginApplyException {
		Pattern pattern = Pattern.compile(String.format(METHOD_REGEX, method), DOTALL | MULTILINE);
		Matcher matcher = pattern.matcher(content);
		if (matcher.find()) {
			int start = matcher.start();
			int end = findEndSymbol(content.toCharArray(), matcher.end(), OPEN_BRACKET) + 1;
			return Pattern.compile('^' + matcher.group(1), MULTILINE)
					.matcher(content.substring(start, end))
					.replaceAll(EMPTY);
		} else {
			throw new PluginApplyException("Cannot find method '" + method + "'");
		}
	}

	private int findEndSymbol(char[] content, int offset, char desiredClosingSymbol) {
		while (offset < content.length) {
			char symbol = content[offset];
			Character symbolPair = mapSeparators.get(desiredClosingSymbol);
			if (symbolPair != null && symbol == symbolPair) {
				return offset;
			} else {
				offset = openSeparators.contains(symbol) ? findEndSymbol(content, ++offset, symbol) : offset;
			}
			offset++;
		}
		return offset;
	}

	@Override
	public String getName() {
		return TAG;
	}
}
