package io.datakernel.docs.plugin.text;

import org.python.util.PythonInterpreter;

import java.util.List;
import java.util.Map;

import static io.datakernel.util.CollectionUtils.map;
import static java.lang.String.format;

public final class HighlightTextPlugin implements TextPlugin {
	private static final String TAG = "highlight";
	private static final String DEFAULT_LANGUAGE = "text";
	private static final Map<String, String> SUPPORT_LANGUAGES = map(
			"text", "TextLexer",
			"java", "JavaLexer",
			"xml", "XmlLexer",
			"bash", "BashLexer");
	private static final String HIGHLIGHT_COMMAND =
			"from pygments import highlight\n"
					+ "from pygments.lexers import %1$s\n"
					+ "from pygments.formatters import HtmlFormatter\n"
					+ "\nresult = highlight(content, %1$s(), HtmlFormatter())";
	private static final String DEFAULT_LEXER = SUPPORT_LANGUAGES.get(DEFAULT_LANGUAGE);
	private static final String CONTENT_BLOCK = "<div class=\"highlight\"><pre>%s</pre></div>";
	private final PythonInterpreter pythonInterpreter;
	private final Object lock = new Object();

	public HighlightTextPlugin(PythonInterpreter pythonInterpreter) {
		this.pythonInterpreter = pythonInterpreter;
	}

	@Override
	public String apply(String innerContent, List<String> params) {
		if (params.size() < 1) {
			return format(CONTENT_BLOCK, innerContent);
		} else {
			String lang = params.get(0);
			synchronized (lock) {
				pythonInterpreter.set("content", innerContent);
				pythonInterpreter.exec(format(HIGHLIGHT_COMMAND, SUPPORT_LANGUAGES.getOrDefault(lang, DEFAULT_LEXER)));
				return pythonInterpreter.get("result", String.class);
			}
		}
	}

	@Override
	public String getName() {
		return TAG;
	}
}
