package io.datakernel.docs.plugin.text;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;

public final class ProjectStructureTextPlugin implements TextPlugin {
	private static final String TAG = "project_structure";
	private static final String START_LINK_TAG = "<a href=\"%s\" target=\"_blank\">";
	private static final String END_LINK_TAG = "</a>";
	private static final String NEW_LINE = "\n";
	private static final String EMPTY = "";

	private static final String PREFIX = "/?(softindex/datakernel/blob/master/)?";
	private final String projectSourceFolderPath;
	private final Path projectSourcePath;
	private final String githubUrl;
	private boolean addLinkFolder;

	public ProjectStructureTextPlugin(Path projectSourcePath, String githubUrl, String projectSourceFolderPath) {
		this.projectSourcePath = projectSourcePath;
		this.projectSourceFolderPath = projectSourceFolderPath;
		this.githubUrl = githubUrl;
	}

	/**
	 * Add html link to folders
	 */
	public ProjectStructureTextPlugin withAddLinkFolder(boolean isAdd) {
		this.addLinkFolder = isAdd;
		return this;
	}

	@Override
	public String apply(String innerContent, List<String> params) throws PluginApplyException {
		if (params.size() < 1) {
			throw new PluginApplyException("Not enough params");
		}
		Path resolvedPath = projectSourcePath.resolve(params.get(0).replaceFirst(PREFIX, EMPTY));
		if (!Files.exists(resolvedPath) || !Files.isDirectory(resolvedPath)) {
			throw new PluginApplyException("File is incorrect");
		}
		return doApply(resolvedPath, 0);
	}

	private String doApply(Path path, int deep) throws PluginApplyException {
		StringBuilder builder = new StringBuilder();
		if (addLinkFolder) {
			String fullUrl = githubUrl + path.toString().replace(projectSourceFolderPath, EMPTY);
			builder.append(format(START_LINK_TAG, fullUrl))
					.append(path.getFileName())
					.append(END_LINK_TAG)
					.append(NEW_LINE);
		} else {
			builder.append(path.getFileName().toString()).append(NEW_LINE);
		}
		try {
			List<Path> subTrees = Files.list(path).collect(toList());
			if (subTrees.isEmpty()) return EMPTY;
			for (Path subPath : subTrees) {
				processSubTrees(builder, subPath, deep);
			}
		} catch (IOException e) {
			throw new PluginApplyException(e);
		}
		return builder.toString();
	}

	private void processSubTrees(StringBuilder builder, Path subPath, int deep) throws PluginApplyException {
		if (Files.isDirectory(subPath)) {
			builder.append(String.join(EMPTY, nCopies(deep, "  ")))
					.append("└──")
					.append(doApply(subPath, deep + 1));
		} else {
			String fullUrl = githubUrl + subPath.toString().replace(projectSourceFolderPath, EMPTY);
			builder.append(String.join(EMPTY, nCopies(deep, "  ")))
					.append("└──")
					.append(format(START_LINK_TAG, fullUrl)).append(subPath.getFileName()).append(END_LINK_TAG)
					.append(NEW_LINE);
		}
	}

	@Override
	public String getName() {
		return TAG;
	}
}
