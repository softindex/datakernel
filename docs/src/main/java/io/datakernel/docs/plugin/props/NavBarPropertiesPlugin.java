package io.datakernel.docs.plugin.props;

import io.datakernel.docs.dao.ResourceDao;
import io.datakernel.docs.model.MarkdownContent;
import io.datakernel.docs.model.NavBar;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.regex.Pattern.DOTALL;
import static java.util.regex.Pattern.MULTILINE;

@SuppressWarnings("WeakerAccess")
public final class NavBarPropertiesPlugin implements PropertiesPlugin<NavBar> {
	private static final String YAML_NAV_BAR = "^-\\s*title:\\s*(?<title>[\\w-]+).*?items:";
	private static final String YAML_NAV_BAR_PARAMS = "\\s*-\\s*(\\w+):\\s+(.*?)\\s*(\\w+):\\s+(.*?)\\s*(\\w+):\\s+(.*?)$";
	private static final Pattern yamlNavBarParamsPattern = Pattern.compile(YAML_NAV_BAR_PARAMS, MULTILINE);
	private static final Pattern yamlNavBarPattern = Pattern.compile(YAML_NAV_BAR, DOTALL | MULTILINE);
	private static final String PREFIX_NAV_FILE = "nav_docs_";
	private static final String EXTENSION_NAV_FILE = ".yml";
	private static final String PLUGIN_NAME = "navBars";
	private final ResourceDao navDataDao;
	private final List<String> sectors;

	private NavBarPropertiesPlugin(List<String> sectors, ResourceDao navDataDao) {
		this.sectors = sectors;
		this.navDataDao = navDataDao;
	}

	public static NavBarPropertiesPlugin create(List<String> sectors, ResourceDao navDataDao) {
		return new NavBarPropertiesPlugin(sectors, navDataDao);
	}

	@Override
	public Map<String, NavBar> apply(@Nullable MarkdownContent markdownContent) throws IOException {
		return markdownContent == null ? readNavBars(emptyMap()) : readNavBars(markdownContent.getProperties());
	}

	@Override
	public String getName() {
		return PLUGIN_NAME;
	}

	public Map<String, NavBar> readNavBars(Map<String, String> properties) throws IOException {
		Map<String, NavBar> navBars = new HashMap<>();
		for (String sector : sectors) {
			String yamlContent = navDataDao.getResource(PREFIX_NAV_FILE + sector + EXTENSION_NAV_FILE);
			navBars.put(sector, NavBar.of(doRead(yamlContent, properties.get("filename"))));
		}
		return navBars;
	}

	private Map<String, List<NavBar.Item>> doRead(String yamlContent, String selectedFilename) {
		Matcher matcher = yamlNavBarPattern.matcher(yamlContent);
		HashMap<String, List<NavBar.Item>> navBar = new HashMap<>();
		boolean isFound = matcher.find();
		if (!isFound) return emptyMap();
		List<String> bars = Arrays.stream(yamlNavBarPattern.split(yamlContent))
				.filter(param -> !param.isEmpty())
				.collect(Collectors.toList());
		for (int i = 0; isFound && i < bars.size(); i++)  {
			String titleDestination = matcher.group("title");
			String params = bars.get(i);
			Matcher paramsMatcher = yamlNavBarParamsPattern.matcher(params);
			List<NavBar.Item> itemList = new ArrayList<>();
			HashMap<String, String> map = new HashMap<>();
			while (paramsMatcher.find()) {
				int count = 1;
				map.put(paramsMatcher.group(count++), paramsMatcher.group(count++));
				map.put(paramsMatcher.group(count++), paramsMatcher.group(count++));
				map.put(paramsMatcher.group(count++), paramsMatcher.group(count));
				itemList.add(NavBar.Item.of(map.get("id"), map.get("filename"), map.get("title"), map.get("filename").equals(selectedFilename)));
				map.clear();
			}
			navBar.put(titleDestination, itemList);
			isFound = matcher.find();
		}
		return navBar;
	}
}
