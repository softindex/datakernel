package io.global.globalfs.api;

import io.datakernel.async.Stage;
import io.global.common.PubKey;

import java.util.List;
import java.util.Set;

/**
 * This interface represents a <b>node-local</b> view/slice of the namespace associated with some public key.
 */
public interface GlobalFsNamespace {
	/**
	 * Since the interface represents only node-local namespace, this returns it's parent node.
	 *
	 * @return the parent node of this namespace
	 */
	GlobalFsNode getNode();

	/**
	 * Namespaces are classified by their public key.
	 *
	 * @return the associated public key
	 */
	PubKey getKey();

	Stage<GlobalFsFileSystem> getFileSystem(String fsName);

	/**
	 * Searches and caches other nodes that might have this namespace on them
	 * using parent's discovery service.
	 *
	 * @return a list of nodes which also contain this namespace
	 */
	Stage<List<GlobalFsNode>> findNodes();

	Stage<Set<String>> getFilesystemNames();
}
