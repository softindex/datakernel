/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.global.fs.api;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.StacklessException;
import io.global.common.PubKey;
import io.global.common.SignedData;

import java.util.List;

import static io.datakernel.file.FileUtils.escapeGlob;
import static io.global.fs.api.MetadataStorage.NO_METADATA;

/**
 * This component handles one of the GlobalFS nodes.
 */
public interface GlobalFsNode {
	StacklessException RECURSIVE_DOWNLOAD_ERROR = new StacklessException(GlobalFsNode.class, "Trying to download a file from a server that also tries to download this file.");
	StacklessException RECURSIVE_UPLOAD_ERROR = new StacklessException(GlobalFsNode.class, "Trying to upload a file to a server that also tries to upload this file.");
	StacklessException FETCH_DID_NOTHING = new StacklessException(GlobalFsNode.class, "Did not fetch anything from given node.");
	StacklessException CANT_VERIFY_METADATA = new StacklessException(GlobalFsNode.class, "Failed to verify signature of the metadata.");

	Promise<ChannelConsumer<DataFrame>> upload(PubKey space, String filename, long offset);

	default ChannelConsumer<DataFrame> uploader(PubKey space, String filename, long offset) {
		return ChannelConsumer.ofPromise(upload(space, filename, offset));
	}

	Promise<ChannelSupplier<DataFrame>> download(PubKey space, String filename, long offset, long limit);

	default ChannelSupplier<DataFrame> downloader(PubKey space, String filename, long offset, long limit) {
		return ChannelSupplier.ofPromise(download(space, filename, offset, limit));
	}

	Promise<List<SignedData<GlobalFsMetadata>>> list(PubKey space, String glob);

	default Promise<SignedData<GlobalFsMetadata>> getMetadata(PubKey space, String filename) {
		return list(space, escapeGlob(filename)).thenCompose(res -> res.size() == 1 ? Promise.of(res.get(0)) : Promise.ofException(NO_METADATA));
	}

	Promise<Void> pushMetadata(PubKey pubKey, SignedData<GlobalFsMetadata> signedMetadata);
}
