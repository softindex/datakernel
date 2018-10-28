package io.datakernel.remotefs;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

class SubfolderFsClient implements FsClient {

	private final FsClient parent;
	private final String folder;

	SubfolderFsClient(FsClient parent, String folder) {
		this.parent = parent;
		this.folder = folder.endsWith(File.separator) ? folder : folder + File.separator;
	}

	@Override
	public Promise<SerialConsumer<ByteBuf>> upload(String filename, long offset) {
		return parent.upload(folder + filename, offset);
	}

	@Override
	public Promise<SerialSupplier<ByteBuf>> download(String filename, long offset, long length) {
		return parent.download(folder + filename, offset, length);
	}

	@Override
	public Promise<Set<String>> move(Map<String, String> changes) {
		return parent.move(changes)
				.thenApply(set ->
						set.stream()
								.map(name -> name.substring(folder.length()))
								.collect(toSet()));
	}

	@Override
	public Promise<Set<String>> copy(Map<String, String> changes) {
		return parent.copy(changes)
				.thenApply(set ->
						set.stream()
								.map(name -> name.substring(folder.length()))
								.collect(toSet()));
	}

	@Override
	public Promise<List<FileMetadata>> list(String glob) {
		return parent.list(folder + glob)
				.thenApply(list ->
						list.stream()
								.map(meta -> new FileMetadata(
										meta.getName().substring(folder.length()),
										meta.getSize(),
										meta.getTimestamp()))
								.collect(toList()));
	}

	@Override
	public Promise<Void> ping() {
		return parent.ping();
	}

	@Override
	public Promise<Void> delete(String glob) {
		return parent.delete(folder + glob);
	}

	@Override
	public FsClient subfolder(String folder) {
		return new SubfolderFsClient(parent, this.folder + folder);
	}
}
