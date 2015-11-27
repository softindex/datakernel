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

package io.datakernel.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.util.ByteBufStrings;

import java.util.*;

import static io.datakernel.http.ContentType.ParentType.*;
import static io.datakernel.util.ByteBufStrings.decodeAscii;

public enum ContentType {
	ATOM(APPLICATION, "atom+xml", "atom"),
	EDI_X12(APPLICATION, "EDI-X12"),
	EDI_EDIFACT(APPLICATION, "EDIFACT"),
	JSON(APPLICATION, "json", "json"),
	JAVASCRIPT_APP(APPLICATION, "javascript"),
	OCTET_STREAM(APPLICATION, "octet-stream", "com", "exe", "bin"),
	ZIP(APPLICATION, "zip", "zip", "zipx"),
	GZIP(APPLICATION, "gzip", "gzip", "gz"),
	BZIP2(APPLICATION, "x-bzip2", "bz2"),
	FLASH(APPLICATION, "x-shockwave-flash"),
	TEX(APPLICATION, "x-tex"),
	PDF(APPLICATION, "pdf", "pdf"),
	OGG_APP(APPLICATION, "ogg", "ogg"),
	POSTSCRIPT(APPLICATION, "postscript", "ai"),
	BINARY(APPLICATION, "binary"),
	TAR(APPLICATION, "x-tar", "tar"),
	KEY_ARCHIVE(APPLICATION, "pkcs12", "p12", "pfx"),
	PROTOBUF(APPLICATION, "protobuf", "proto"),
	EPUB(APPLICATION, "epub+zip", "epub"),
	RDF(APPLICATION, "rdf+xml", "rdf"),
	XRD(APPLICATION, "xrd+xml", "xrd"),
	JAVA_ARCHIVE(APPLICATION, "java-archive", "jar", "war", "ear"),
	WOFF(APPLICATION, "font-woff", "woff"),
	EOT(APPLICATION, "vnd.ms-fontobject", "eot"),
	SFNT(APPLICATION, "font-sfnt", "sfnt"),

	CSS(TEXT, "css", "css"),
	CSV(TEXT, "csv", "csv"),
	HTML(TEXT, "html", "html", "htm"),
	PLAIN_TEXT(TEXT, "plain", "txt"),
	RTF(TEXT, "rtf", "rtf"),
	XML(TEXT, "xml", "xml"),
	XHTML(TEXT, "xhtml+xml", "xhtml"),
	JAVASCRIPT_TXT(TEXT, "javascript", "js"),

	BMP(IMAGE, "bmp", "bmp"),
	ICO(IMAGE, "vnd.microsoft.icon", "ico"),
	CRW(IMAGE, "x-canon-crw", "crw"),
	PSD(IMAGE, "vnd.adobe.photoshop", "psd"),
	WEBP(IMAGE, "webp", "webp"),
	GIF(IMAGE, "gif", "gif"),
	JPEG(IMAGE, "jpeg", "jpeg", "jpg"),
	PNG(IMAGE, "png", "png"),
	SVG(IMAGE, "svg+xml", "svg"),
	TIFF(IMAGE, "tiff", "tiff", "tif"),

	OGG_AUDIO(AUDIO, "ogg", "oga"),
	MP4_AUDIO(AUDIO, "mp4", "mp4a"),
	MPEG_AUDIO(AUDIO, "mpeg", "mpga"),
	WEBM_AUDIO(AUDIO, "webm", "weba"),
	FLAC(AUDIO, "x-flac", "flac"),
	MP3(AUDIO, "mp3", "mp3"),
	AAC(AUDIO, "aac", "aac"),
	WMA(AUDIO, "x-ms-wma", "wma"),
	REALAUDIO(AUDIO, "vnd.rn-realaudio", "rm"),
	WAVE(AUDIO, "vnd.wave", "wave", "wav"),

	MPEG(VIDEO, "mpeg", "mpeg", "mpg"),
	MP4(VIDEO, "mp4", "mp4"),
	QUICKTIME(VIDEO, "quicktime", "mov", "moov", "qt", "qtvr"),
	OGG_VIDEO(VIDEO, "ogg", "ogv"),
	WEBM(VIDEO, "webm", "webm"),
	WMV(VIDEO, "x-ms-wmv", "wmv"),
	FLV(VIDEO, "x-flv", "flv"),
	AVI(VIDEO, "avi", "avi");

	public enum ParentType {
		APPLICATION, AUDIO, EXAMPLE, IMAGE, MESSAGE, MODEL, MULTIPART, TEXT, VIDEO
	}

	private static final Map<String, ContentType> TYPES = new HashMap<>();

	private static final Map<String, ContentType> EXT = new HashMap<>();

	static {
		for (ContentType contentType : ContentType.values()) {
			TYPES.put(contentType.stringValue, contentType);
			for (String extension : contentType.extension) {
				EXT.put(extension, contentType);
			}
		}
	}

	private final String stringValue;
	private final List<String> extension = new ArrayList<>();

	ContentType(ParentType parentType, String subtype) {
		this.stringValue = parentType.name().toLowerCase() + '/' + subtype;
	}

	ContentType(ParentType parentType, String subtype, String... extensions) {
		this.stringValue = parentType.name().toLowerCase() + '/' + subtype;
		this.extension.addAll(Arrays.asList(extensions));
	}

	static void parse(String contentTypes, List<ContentType> list) {
		byte[] bytes = ByteBufStrings.encodeAscii(contentTypes);
		parse(bytes, 0, bytes.length, list);
	}

	static void parse(ByteBuf buf, List<ContentType> list) {
		parse(buf.array(), buf.position(), buf.limit(), list);
	}

	static void parse(byte[] bytes, int pos, int end, List<ContentType> list) {
		// parsing wo parameters
		while (pos < end) {
			int start = pos;
			while (pos < end && !(bytes[pos] == ';' || bytes[pos] == ',')) {
				pos++;
			}
			list.add(parse(decodeAscii(bytes, start, pos - start)));
			// skipping parameters
			if (pos < end && bytes[pos] == ';') {
				while (pos < end && bytes[pos] != ',') {
					pos++;
				}
			}
			pos += 2;
		}

	}

	static ContentType parse(String headerString) {
		return TYPES.get(headerString);
	}

	static String render(List<ContentType> types) {
		int estimateSize = types.size() * 25;
		ByteBuf buf = ByteBuf.allocate(estimateSize);
		render(types, buf);
		buf.flip();
		return decodeAscii(buf);
	}

	static void render(List<ContentType> types, ByteBuf buf) {
		int pos = render(types, buf.array(), buf.position());
		buf.position(pos);
	}

	static int render(List<ContentType> types, byte[] bytes, int pos) {
		// wo parameters
		for (int i = 0; i < types.size(); i++) {
			byte[] string = ByteBufStrings.encodeAscii(types.get(i).getStringValue());
			System.arraycopy(string, 0, bytes, pos, string.length);
			pos += string.length;
			if (i != types.size() - 1) {
				bytes[pos++] = ',';
				bytes[pos] = ' ';
			}
			pos++;
		}
		return pos;
	}

	public static ContentType getByExt(String ext) {
		return EXT.get(ext);
	}

	public String getStringValue() {
		return stringValue;
	}

	@Override
	public String toString() {
		return stringValue;
	}
}