package io.datakernel.http;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is a util class that provides a mean to render any exception into an {@link HttpResponse}
 * with the stacktrace rendered nicely.
 * It also generates link to the IntelliJ IDEA REST API (http://localhost:63342) from the stacktrace,
 * just like IDEA does in its log console.
 */
public final class DebugStacktraceRenderer {
	private static final String IDEA_REST_API_URL = "http://localhost:63342";
	private static final String DEBUG_SERVER_ERROR_HTML = "<!doctype html>" +
			"<html lang=\"en\">" +
			"<head>" +
			"<meta charset=\"UTF-8\">" +
			"<title>Internal Server Error</title>" +
			"<style>" +
			"html, body { height: 100%; margin: 0; padding: 0; }" +
			"h1, p { font-family: sans-serif; }" +
			".link { color: #00E; text-decoration: underline; cursor: pointer; user-select: none; }" +
			"</style>" +
			"</head>" +
			"<body>" +
			"<script>" +
			"window.onload = () => fetch('" + IDEA_REST_API_URL + "').then(() => document.querySelectorAll('[data-link]').forEach(a => {" +
			"a.onclick = () => fetch(a.dataset.link);" +
			"a.classList.add('link');" +
			"}));" +
			"</script>" +
			"<div style=\"position:relative;min-height:100%;\">" +
			"<h1 style=\"text-align:center;margin-top:0;padding-top:0.5em;\">Internal Server Error</h1>" +
			"<hr style=\"margin-left:10px;margin-right:10px;\">" +
			"<pre style=\"color:#8B0000;font-size:1.5em;padding:10px 10px 4em;\">{stacktrace}</pre>" +
			"<div style=\"position:absolute;bottom:0;width:100%;height:4em\">" +
			"<hr style=\"margin-left:10px;margin-right:10px\">" +
			"<p style=\"text-align:center;\">DataKernel 3.0.0</p>" +
			"</div>" +
			"</div>" +
			"</body>" +
			"</html>";

	private static final Pattern STACK_TRACE_ELEMENT;

	static {
		String ident = "\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*";
		STACK_TRACE_ELEMENT = Pattern.compile("(at ((?:" + ident + "\\.)+)" + ident + "\\()(" + ident + "(\\." + ident + ")(:\\d+)?)\\)");
	}

	private DebugStacktraceRenderer() {}

	public static HttpResponse render(Throwable e) {
		StringWriter writer = new StringWriter();
		e.printStackTrace(new PrintWriter(writer));
		Matcher matcher = STACK_TRACE_ELEMENT.matcher(writer.toString());
		StringBuffer stacktrace = new StringBuffer();
		while (matcher.find()) {
			String cls = matcher.group(2);
			String quotedFile = Matcher.quoteReplacement(cls.substring(0, cls.length() - 1).replace('.', '/').replaceAll("\\$.*(?:\\.|$)", ""));
			matcher.appendReplacement(stacktrace, "$1<a data-link=\"" + IDEA_REST_API_URL + "/api/file/" + quotedFile + "$4$5\">$3</a>)");
		}
		matcher.appendTail(stacktrace);
		return HttpResponse.ofCode(500)
				.withHtml(DEBUG_SERVER_ERROR_HTML.replace("{stacktrace}", stacktrace));
	}
}
