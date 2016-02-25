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

package io.datakernel.examples;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * First we define the structure of the input record which represents the word and id of the document that contains this word.
 */
public class InvertedIndexRecord {
	public String word;
	public Integer documentId;

	public InvertedIndexRecord() {
	}

	public InvertedIndexRecord(String word, Integer documentId) {
		this.word = word;
		this.documentId = documentId;
	}

	public static final List<String> KEYS = asList("word");

	public static final List<String> INPUT_FIELDS = asList("documentId");

	public static final List<String> OUTPUT_FIELDS = asList("documents");

	public static final Map<String, String> OUTPUT_TO_INPUT_FIELDS = ImmutableMap.of("documents", "documentId");

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		InvertedIndexRecord that = (InvertedIndexRecord) o;

		if (word != null ? !word.equals(that.word) : that.word != null) return false;
		return !(documentId != null ? !documentId.equals(that.documentId) : that.documentId != null);

	}

	@Override
	public int hashCode() {
		int result = word != null ? word.hashCode() : 0;
		result = 31 * result + (documentId != null ? documentId.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("word", word)
				.add("documentId", documentId)
				.toString();
	}
}
