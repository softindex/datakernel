/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.hashfs;

import io.datakernel.async.CompletionCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.lang.Math.max;

public class DirectoryMetadata {
	private static final Logger logger = LoggerFactory.getLogger(DirectoryMetadata.class);

	private static final String IN_PROGRESS_EXTENSION = ".partial";

	private final Path root;
	private final ServerInfo myId;
	private final Configuration configuration;

	private final Map<String, FileMetadata> filesMetadata = new HashMap<>();

	// TODO save replica files for quickly clear on replica fail

	private DirectoryMetadata(Path root, ServerInfo myId, Configuration configuration) {
		this.root = root;
		this.myId = myId;
		this.configuration = configuration;
	}

	public static DirectoryMetadata createDirectory(Path fileStorage, ServerInfo myId) {
		DirectoryMetadata metadata = new DirectoryMetadata(fileStorage, myId, new Configuration());
		metadata.scanFiles(fileStorage);
		return metadata;
	}

	public static DirectoryMetadata createDirectory(Path fileStorage, ServerInfo myId, Configuration configuration) {
		DirectoryMetadata metadata = new DirectoryMetadata(fileStorage, myId, configuration);
		metadata.scanFiles(fileStorage);
		return metadata;
	}

	public Map<String, FileMetadata.FileBaseInfo> getFileBaseInfo() {
		Map<String, FileMetadata.FileBaseInfo> filesBaseInfo = new HashMap<>();
		for (Map.Entry<String, FileMetadata> metaFiles : filesMetadata.entrySet()) {
			if (metaFiles.getValue().status != FileMetadata.FileStatus.IN_PROGRESS) {
				filesBaseInfo.put(metaFiles.getKey(), metaFiles.getValue().getFileBaseInfo());
			}
		}
		return filesBaseInfo;
	}

	private void scanFiles(Path storage) {
		if (!Files.exists(storage)) {
			try {
				Files.createDirectories(storage);
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
				return;
			}
		}

		if (Files.isDirectory(storage)) {
			try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(storage)) {

				// TODO remove empty dirs ??
				for (Path path : directoryStream) {
					if (Files.isDirectory(path)) {
						scanFiles(path);
					} else {
						Path relative = root.relativize(path);
						String filename = relative.toString();
						String fileEnding = filename.contains(".") ? filename.substring(filename.lastIndexOf(".")) : "";
						switch (fileEnding) {
							case IN_PROGRESS_EXTENSION:
								// remove not full loaded files
								try {
									Files.delete(path);
								} catch (IOException ex) {
									logger.error("Can't remove file {} on server {} ", path.toAbsolutePath(), myId.serverId, ex);
								}
								break;
							default:
								long fileSize = Files.size(path);
								setFileStatusReady(filename, fileSize);
						}
					}
				}
			} catch (IOException ex) {
				logger.error("Can't read file hierarchy on server {}", myId.serverId, ex);
			}
		}
	}

	public boolean fileExists(String filename) {
		return filesMetadata.containsKey(filename) && filesMetadata.get(filename).status == FileMetadata.FileStatus.READY;
	}

	public boolean isFileDeletedByUser(String filename) {
		return filesMetadata.containsKey(filename) && filesMetadata.get(filename).status == FileMetadata.FileStatus.TOMBSTONE;
	}

	public boolean canUpload(String filename) {
		return !filesMetadata.containsKey(filename);
	}

	public void startUpload(String filename) {
		filesMetadata.put(filename, new FileMetadata(FileMetadata.FileStatus.IN_PROGRESS));
	}

	public void deleteUploadFailedFile(String filename) {
		filesMetadata.remove(filename);
	}

	public boolean uploadFinish(String filename) {

		// Rename file from "filename.partial" to "filename"
		try {
			Path uploadPath = getUploadFilePath(filename);
			Path destination = getFilePath(filename);
			Files.move(uploadPath, destination);
			long fileSize = Files.size(destination);
			setFileStatusReady(filename, fileSize);
			return true;
		} catch (IOException e) {
			logger.error("Error while renaming file {} on {}", filename, myId.serverId, e);
			try {
				Path uploadPath = getUploadFilePath(filename);
				// delete file if it can't be renamed
				if (Files.exists(uploadPath)) {
					Files.delete(uploadPath);
				}
			} catch (IOException e1) {
				logger.error(e.getMessage(), e1);
			}
			// on error remove file from memory
			filesMetadata.remove(filename);
		}

		return false;
	}

	public void updateReplica(ServerInfo replica, String filename) {
		if (filesMetadata.containsKey(filename) && filesMetadata.get(filename).status != FileMetadata.FileStatus.TOMBSTONE) {
			filesMetadata.get(filename).addReplica(replica);
		}
	}

	private void setFileStatusReady(String filename, long fileSize) {
		if (!filesMetadata.containsKey(filename)) {
			filesMetadata.put(filename, new FileMetadata(FileMetadata.FileStatus.READY));
		} else {
			filesMetadata.get(filename).updateStatus(FileMetadata.FileStatus.READY, fileSize);
		}

		filesMetadata.get(filename).addReplica(myId);
	}

	public void deleteFileByServer(String filename) {
		if (filesMetadata.containsKey(filename)) {
			try {
				Path pathToFile = getFilePath(filename);
				if (Files.exists(pathToFile)) {
					Files.delete(pathToFile);
				}
			} catch (IOException e) {
				logger.error("Can't delete file {} by server", filename, e);
			}
			filesMetadata.remove(filename);
		}
	}

	public void deleteFileByUser(String filename, CompletionCallback callback) {
		if (filesMetadata.containsKey(filename) && filesMetadata.get(filename).status == FileMetadata.FileStatus.READY) {
			switch (filesMetadata.get(filename).status) {
				case READY:
					try {
						Path pathToFile = getFilePath(filename);
						Files.delete(pathToFile);
					} catch (IOException e) {
						logger.error("FileServer {} failed remove file {}", myId.serverId, filename, e);
						callback.onException(e);
						return;
					}
					filesMetadata.get(filename).status = FileMetadata.FileStatus.TOMBSTONE;
					filesMetadata.get(filename).clearReplicas();
					break;
				case IN_PROGRESS: // TODO (eberezhanskyi): dead code?
					callback.onException(new Exception("File in progress can't be deleted."));
					return;
				case TOMBSTONE:
					callback.onComplete();
					return;
			}
		}
		callback.onComplete();
	}

	public Path getFilePath(String file) throws IOException {
		return getFilePath(file, false);
	}

	private Path getUploadFilePath(String file) throws IOException {
		return getFilePath(file, true);
	}

	public String normalisePath(String filename) {
		if (filename.contains("\\")) {
			filename = filename.replaceAll("\\\\", "/");
		}
		return filename;
	}

	public Path getFilePath(String filename, boolean forUpload) throws IOException {
		if (!File.separator.equals("/") && filename.contains("/")) {
			filename = filename.replaceAll("/", "\\\\");
		}

		int lastIndex = max(filename.lastIndexOf('/'), filename.lastIndexOf('\\'));
		if (forUpload && lastIndex != -1) {
			String dir = filename.substring(0, lastIndex);
			Files.createDirectories(root.resolve(Paths.get(dir)));
		}

		return forUpload ? root.resolve(filename + IN_PROGRESS_EXTENSION) : root.resolve(filename);
	}

	public List<String> fileList() {
		List<String> filenames = new ArrayList<>();

		for (Map.Entry<String, FileMetadata> fileMetaEntry : filesMetadata.entrySet()) {
			String name = fileMetaEntry.getKey();
			FileMetadata.FileStatus fileStatus = fileMetaEntry.getValue().status;
			if (fileStatus == FileMetadata.FileStatus.READY) {
				filenames.add(name);
			}
		}
		return filenames;
	}

	public Map<String, Long> fileSizes() {
		Map<String, Long> filenames = new HashMap<>();

		for (Map.Entry<String, FileMetadata> fileMetaEntry : filesMetadata.entrySet()) {
			String name = fileMetaEntry.getKey();
			FileMetadata.FileStatus fileStatus = fileMetaEntry.getValue().status;
			if (fileStatus == FileMetadata.FileStatus.READY) {
				filenames.put(name, fileMetaEntry.getValue().getFileSize());
			}
		}
		return filenames;
	}

	public Set<ServerInfo> getReplicas(String filename) {
		if (filesMetadata.containsKey(filename)) {
			return filesMetadata.get(filename).getReplicas();
		}
		return new HashSet<>();
	}

	public ReshardFiles getReshardInfo(ServerInfo remoteServer, List<ServerInfo> aliveServers) {

		Map<String, FileMetadata.FileBaseInfo> forReplicate = new HashMap<>();
		Set<String> forDelete = new HashSet<>();

		for (Map.Entry<String, FileMetadata> metaFiles : filesMetadata.entrySet()) {
			String filename = metaFiles.getKey();
			FileMetadata currentStatus = metaFiles.getValue();
			if (currentStatus.status != FileMetadata.FileStatus.IN_PROGRESS) {

				List<ServerInfo> orderedServers = RendezvousHashing.sortServers(aliveServers, filename);
				orderedServers = orderedServers.subList(0, Math.min(orderedServers.size(), configuration.getReplicas()));

				boolean isShouldBeHere = orderedServers.size() < configuration.getReplicas() || orderedServers.contains(myId);
				boolean isShouldBeRemote = orderedServers.contains(remoteServer);

				if (currentStatus.status == FileMetadata.FileStatus.READY) {
					Set<ServerInfo> replicas = currentStatus.getReplicas();
					// if enough replicas and not our file
					if (!isShouldBeHere && replicas.size() >= configuration.getReplicas() + 1) {
						forDelete.add(filename);
					}
				}

				if (isShouldBeRemote) {
					forReplicate.put(filename, currentStatus.getFileBaseInfo());
				}
			}
		}
		return new ReshardFiles(forDelete, forReplicate);
	}

	public Map<String, Set<Integer>> getFilesWithReplicas(List<String> interestingFiles) {
		Map<String, Set<Integer>> replicaFiles = new HashMap<>();

		if (interestingFiles.isEmpty()) {
			for (Map.Entry<String, FileMetadata> metaEntry : filesMetadata.entrySet()) {
				Set<Integer> replicas = new HashSet<>();
				for (ServerInfo info : metaEntry.getValue().getReplicas()) {
					replicas.add(info.serverId);
				}
				replicaFiles.put(metaEntry.getKey(), replicas);
			}
		} else {
			for (String file : interestingFiles) {
				if (fileExists(file)) {
					Set<Integer> replicas = new HashSet<>();
					for (ServerInfo info : filesMetadata.get(file).getReplicas()) {
						replicas.add(info.serverId);
					}
					replicaFiles.put(file, replicas);
				}
			}
		}

		return replicaFiles;
	}

	public void removeReplica(ServerInfo serverInfo) {
		// remove all replicas with this server
		// rebalancing will start automatically on next tick

		for (FileMetadata currentReplicas : filesMetadata.values()) {
			currentReplicas.removeReplica(serverInfo);
		}
	}

	public void removeFileReplica(ServerInfo serverInfo, String filename) {
		if (filesMetadata.containsKey(filename)) {
			filesMetadata.get(filename).removeReplica(serverInfo);
		}
	}

	/**
	 * If network is down remove info about replicas
	 */
	public void clearAllReplicas() {
		for (FileMetadata currentReplicas : filesMetadata.values()) {
			currentReplicas.clearReplicas();
			currentReplicas.addReplica(myId);
		}
	}

	@Override
	public String toString() {
		StringBuilder directoryState = new StringBuilder();

		directoryState.append("Servers status\t");
		directoryState.append(myId.serverId);
		directoryState.append(":\n");

		for (Map.Entry<String, FileMetadata> fileMeta : filesMetadata.entrySet()) {
			directoryState.append("\t");
			String filename = fileMeta.getKey();
			FileMetadata status = fileMeta.getValue();
			directoryState.append(filename);
			directoryState.append("\t");
			directoryState.append(status.status);
			directoryState.append("\t");
			directoryState.append(status.getFileSize());
			directoryState.append("\treplicas:\t");
			for (ServerInfo replica : status.getReplicas()) {
				directoryState.append(replica.serverId);
				directoryState.append("\t");
			}
			directoryState.append("\n");
		}

		return directoryState.toString();
	}

	public static class ReshardFiles {
		public final Set<String> filesForDelete; // only if we have N replicas
		public final Map<String, FileMetadata.FileBaseInfo> filesForDistribute; // key - id_server for distribute

		public ReshardFiles(Set<String> filesForDelete, Map<String, FileMetadata.FileBaseInfo> filesForDistribute) {
			this.filesForDelete = filesForDelete;
			this.filesForDistribute = filesForDistribute;
		}
	}
}
