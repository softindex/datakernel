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

import java.util.HashMap;
import java.util.Map;

import static io.datakernel.util.ByteBufStrings.*;

@SuppressWarnings("unused")
public final class MediaType extends CaseInsensitiveTokenMap.Token {
	private static final CaseInsensitiveTokenMap<MediaType> mediaTypes = new CaseInsensitiveTokenMap<MediaType>(2048, 2, MediaType.class) {
		@Override
		protected MediaType create(byte[] bytes, int offset, int length, byte[] lowerCaseBytes, int lowerCaseHashCode) {
			return new MediaType(bytes, offset, length, bytes, lowerCaseHashCode);
		}
	};
	private static final Map<String, MediaType> extensions = new HashMap<>();

	public static final MediaType ANY = mediaTypes.register("*/*");
	public static final MediaType ANY_APPLICATION = mediaTypes.register("application/*");
	public static final MediaType ANY_TEXT = mediaTypes.register("text/*");
	public static final MediaType ANY_IMAGE = mediaTypes.register("image/*");
	public static final MediaType ANY_AUDIO = mediaTypes.register("audio/*");
	public static final MediaType ANY_VIDEO = mediaTypes.register("video/*");

	public static final MediaType ATOM = mediaTypes.register("application/atom+xml").addExtensions("atom");
	public static final MediaType EDI_X12 = mediaTypes.register("application/EDI-X12");
	public static final MediaType EDI_EDIFACT = mediaTypes.register("application/EDIFACT");
	public static final MediaType JSON = mediaTypes.register("application/json").addExtensions("json");
	public static final MediaType JAVASCRIPT_APP = mediaTypes.register("application/javascript").addExtensions("js");
	public static final MediaType OCTET_STREAM = mediaTypes.register("application/octet-stream").addExtensions("com", "exe", "bin");
	public static final MediaType ZIP = mediaTypes.register("application/zip").addExtensions("zip", "zipx");
	public static final MediaType GZIP = mediaTypes.register("application/gzip").addExtensions("gzip", "gz");
	public static final MediaType BZIP2 = mediaTypes.register("application/x-bzip2").addExtensions("bz2");
	public static final MediaType FLASH = mediaTypes.register("application/x-shockwave-flash");
	public static final MediaType TEX = mediaTypes.register("application/x-tex");
	public static final MediaType PDF = mediaTypes.register("application/pdf").addExtensions("pdf");
	public static final MediaType OGG_APP = mediaTypes.register("application/ogg").addExtensions("ogg");
	public static final MediaType POSTSCRIPT = mediaTypes.register("application/postscript").addExtensions("ai");
	public static final MediaType BINARY = mediaTypes.register("application/binary");
	public static final MediaType TAR = mediaTypes.register("application/x-tar").addExtensions("tar");
	public static final MediaType KEY_ARCHIVE = mediaTypes.register("application/pkcs12").addExtensions("p12", "pfx");
	public static final MediaType PROTOBUF = mediaTypes.register("application/protobuf").addExtensions("proto");
	public static final MediaType EPUB = mediaTypes.register("application/epub+zip").addExtensions("epub");
	public static final MediaType RDF = mediaTypes.register("application/rdf+xml").addExtensions("rdf");
	public static final MediaType XRD = mediaTypes.register("application/xrd+xml").addExtensions("xrd");
	public static final MediaType JAVA_ARCHIVE = mediaTypes.register("application/java-archive").addExtensions("jar", "war", "ear");
	public static final MediaType WOFF = mediaTypes.register("application/font-woff").addExtensions("woff");
	public static final MediaType EOT = mediaTypes.register("application/vnd.ms-fontobject").addExtensions("eot");
	public static final MediaType SFNT = mediaTypes.register("application/font-sfnt").addExtensions("sfnt");
	public static final MediaType XML_APP = mediaTypes.register("application/xml");
	public static final MediaType XHTML_APP = mediaTypes.register("application/xhtml+xml");

	public static final MediaType CSS = mediaTypes.register("text/css").addExtensions("css");
	public static final MediaType CSV = mediaTypes.register("text/csv").addExtensions("csv");
	public static final MediaType HTML = mediaTypes.register("text/html").addExtensions("html", "htm");
	public static final MediaType PLAIN_TEXT = mediaTypes.register("text/plain").addExtensions("txt");
	public static final MediaType RTF = mediaTypes.register("text/rtf").addExtensions("rtf");
	public static final MediaType XML = mediaTypes.register("text/xml").addExtensions("xml");
	public static final MediaType XHTML = mediaTypes.register("text/xhtml+xml").addExtensions("xhtml");
	public static final MediaType JAVASCRIPT_TXT = mediaTypes.register("text/javascript");

	public static final MediaType BMP = mediaTypes.register("image/bmp").addExtensions("bmp");
	public static final MediaType ICO = mediaTypes.register("image/vnd.microsoft.icon").addExtensions("ico");
	public static final MediaType CRW = mediaTypes.register("image/x-canon-crw").addExtensions("crw");
	public static final MediaType PSD = mediaTypes.register("image/vnd.adobe.photoshop").addExtensions("psd");
	public static final MediaType WEBP = mediaTypes.register("image/webp").addExtensions("webp");
	public static final MediaType GIF = mediaTypes.register("image/gif").addExtensions("gif");
	public static final MediaType JPEG = mediaTypes.register("image/jpeg").addExtensions("jpeg", "jpg");
	public static final MediaType PNG = mediaTypes.register("image/png").addExtensions("png");
	public static final MediaType SVG = mediaTypes.register("image/svg+xml").addExtensions(").addExtensions(g");
	public static final MediaType TIFF = mediaTypes.register("image/tiff").addExtensions("tiff", "tif");

	public static final MediaType OGG_AUDIO = mediaTypes.register("audio/ogg").addExtensions("oga");
	public static final MediaType MP4_AUDIO = mediaTypes.register("audio/mp4").addExtensions("mp4a");
	public static final MediaType MPEG_AUDIO = mediaTypes.register("audio/mpeg").addExtensions("mpga");
	public static final MediaType WEBM_AUDIO = mediaTypes.register("audio/webm").addExtensions("weba");
	public static final MediaType FLAC = mediaTypes.register("audio/x-flac").addExtensions("flac");
	public static final MediaType MP3 = mediaTypes.register("audio/mp3").addExtensions("mp3");
	public static final MediaType AAC = mediaTypes.register("audio/aac").addExtensions("aac");
	public static final MediaType WMA = mediaTypes.register("audio/x-ms-wma").addExtensions("wma");
	public static final MediaType REALAUDIO = mediaTypes.register("audio/vnd.rn-realaudio").addExtensions("rm");
	public static final MediaType WAVE = mediaTypes.register("audio/vnd.wave").addExtensions("wave", "wav");

	public static final MediaType MPEG = mediaTypes.register("video/mpeg").addExtensions("mpeg", "mpg");
	public static final MediaType MP4 = mediaTypes.register("video/mp4").addExtensions("mp4");
	public static final MediaType QUICKTIME = mediaTypes.register("video/quicktime").addExtensions("mov", "moov", "qt", "qtvr");
	public static final MediaType OGG_VIDEO = mediaTypes.register("video/ogg").addExtensions("ogv");
	public static final MediaType WEBM = mediaTypes.register("video/webm").addExtensions("webm");
	public static final MediaType WMV = mediaTypes.register("video/x-ms-wmv").addExtensions("wmv");
	public static final MediaType FLV = mediaTypes.register("video/x-flv").addExtensions("flv");
	public static final MediaType AVI = mediaTypes.register("video/avi").addExtensions("avi");

	protected final byte[] bytes;
	protected final int offset;
	protected final int length;

	protected MediaType(byte[] bytes, int offset, int length, byte[] lowerCaseBytes, int lowerCaseHashCode) {
		this.bytes = bytes;
		this.offset = offset;
		this.length = length;
		this.lowerCaseBytes = bytes; // should equal
		this.lowerCaseHashCode = lowerCaseHashCode;
	}

	private MediaType addExtensions(String... extensions) {
		for (String extension : extensions) {
			MediaType.extensions.put(extension, this);
		}
		return this;
	}

	public static MediaType getByName(String name) {
		byte[] array = encodeAscii(name);
		return new MediaType(array, 0, array.length, null, hashCodeLowerCaseAscii(array));
	}

	public static MediaType getByExt(String ext) {
		return extensions.get(ext);
	}

	static MediaType parse(String name) {
		byte[] array = encodeAscii(name);
		return parse(array, 0, array.length, hashCodeLowerCaseAscii(array));
	}

	static MediaType parse(byte[] bytes, int offset, int length, int lowerCaseHashCode) {
		return mediaTypes.get(bytes, offset, length, lowerCaseHashCode);
	}

	void render(ByteBuf buf) {
		int len = render(buf.array(), buf.position());
		buf.advance(len);
	}

	int render(byte[] container, int pos) {
		System.arraycopy(bytes, offset, container, pos, length);
		return length;
	}

	boolean isText() {
		return bytes.length > 5
				&& bytes[0] == 't'
				&& bytes[1] == 'e'
				&& bytes[2] == 'x'
				&& bytes[3] == 't'
				&& bytes[4] == '/';
	}

	int estimateSize() {
		return bytes.length;
	}

	@Override
	public String toString() {
		return decodeAscii(bytes);
	}
}