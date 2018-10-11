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

import io.datakernel.async.Stage;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;

import java.util.List;

/**
 * This component handles one of the GlobalFS nodes.
 */
public interface GlobalFsNode {
	GlobalFsException RECURSIVE_DOWNLOAD_ERROR = new GlobalFsException(GlobalFsNode.class, "Trying to download a file from a server that also tries to download this file.");
	GlobalFsException RECURSIVE_UPLOAD_ERROR = new GlobalFsException(GlobalFsNode.class, "Trying to upload a file to a server that also tries to upload this file.");
	GlobalFsException FETCH_DID_NOTHING = new GlobalFsException(GlobalFsNode.class, "Did not fetch anything from given node.");
	GlobalFsException CANT_VERIFY_METADATA = new GlobalFsException(GlobalFsNode.class, "Failed to verify signature of the metadata.");
	GlobalFsException FILE_NOT_FOUND = new GlobalFsException(GlobalFsNode.class, "Did not found the requested file on given node.");

	RawServerId getId();

	Stage<SerialConsumer<DataFrame>> upload(GlobalFsPath path, long offset);

	default SerialConsumer<DataFrame> uploader(GlobalFsPath path, long offset) {
		return SerialConsumer.ofStage(upload(path, offset));
	}

	Stage<SerialSupplier<DataFrame>> download(GlobalFsPath path, long offset, long limit);

	default SerialSupplier<DataFrame> downloader(GlobalFsPath path, long offset, long limit) {
		return SerialSupplier.ofStage(download(path, offset, limit));
	}

	default Stage<SignedData<GlobalFsMetadata>> getMetadata(GlobalFsPath path) {
		return list(path.getSpace(), path.getPath()).thenApply(res -> res.size() == 1 ? res.get(0) : null);
	}

	Stage<List<SignedData<GlobalFsMetadata>>> list(GlobalFsSpace space, String glob);

	Stage<Void> pushMetadata(PubKey pubKey, SignedData<GlobalFsMetadata> signedMetadata);
}
