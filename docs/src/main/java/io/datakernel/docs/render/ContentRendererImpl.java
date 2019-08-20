package io.datakernel.docs.render;

import com.github.mustachejava.MustacheFactory;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.docs.dao.MarkdownDao;
import io.datakernel.docs.model.MarkdownContent;
import io.datakernel.docs.plugin.TextEngineRenderer;
import io.datakernel.docs.plugin.props.PropertiesPlugin;
import io.datakernel.writer.ByteBufWriter;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.datakernel.util.CollectionUtils.map;

public class ContentRendererImpl implements ContentRenderer {
	private static final String SECTOR_PATTERN = ".*?/([\\w-]+)(/tutorials)?(/[\\w-]+\\.\\w+)?$";
	private final List<PropertiesPlugin<?>> propertiesPluginList = new ArrayList<>();
	private final Pattern sectorPattern = Pattern.compile(SECTOR_PATTERN);
	private final String pathToIncludeFileMustache;
	private final TextEngineRenderer textRenderer;
	private final MustacheFactory mustache;
	private final MarkdownDao markdownDao;
	private final Set<String> sectors;
	private String indexRelativePathPath = "/";

	private ContentRendererImpl(MarkdownDao markdownDao, TextEngineRenderer textRenderer,
								MustacheFactory mustache, String pathToIncludeFileMustache,
								Set<String> sectors) {
		this.markdownDao = markdownDao;
		this.mustache = mustache;
		this.textRenderer = textRenderer;
		this.pathToIncludeFileMustache = pathToIncludeFileMustache;
		this.sectors = sectors;
	}

	public static ContentRendererImpl create(MarkdownDao markdownDao, TextEngineRenderer textRenderer,
											 MustacheFactory mustache, String pathToIncludeFileMustache,
											 Set<String> sectors) {
		return new ContentRendererImpl(markdownDao, textRenderer, mustache, pathToIncludeFileMustache, sectors);
	}

	public ContentRendererImpl withPropertiesPlugins(Collection<PropertiesPlugin<?>> propertiesPlugins) {
		this.propertiesPluginList.addAll(propertiesPlugins);
		return this;
	}

	public ContentRendererImpl withIndexPage(String indexUrl) {
		this.indexRelativePathPath = indexUrl;
		return this;
	}

	@Override
	public ByteBuf render(@NotNull Path templatePath, @NotNull String relativePath) throws IOException, ContentRenderException {
		Map<String, Object> properties = new HashMap<>();
		if (!markdownDao.exist(relativePath)) {
			throw new ContentRenderException("Cannot find relative path " + relativePath);
		}
		MarkdownContent markdownContent = markdownDao.loadContent(relativePath);
		for (PropertiesPlugin<?> plugin : propertiesPluginList) {
			properties.put(plugin.getName(), plugin.apply(markdownContent));
		}
		String renderedContent = textRenderer.render(markdownContent.getContent());
		if (relativePath.contains(indexRelativePathPath)) properties.put("page.home", true);
		Matcher matcher = sectorPattern.matcher(relativePath);
		if (matcher.find()) {
			String sector = matcher.group(1);
			properties.put("active", map(sector, sectors.contains(sector)));
		}
		properties.put("renderedContent", renderedContent);
		properties.put("page", markdownContent);

		StringReader templateReader = new StringReader(new String(Files.readAllBytes(templatePath)));
		ByteBufWriter writer = new ByteBufWriter();
		mustache.compile(templateReader, pathToIncludeFileMustache).execute(writer, properties);
		return writer.getBuf();
	}
}
