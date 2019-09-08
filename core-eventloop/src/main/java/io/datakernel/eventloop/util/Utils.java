package io.datakernel.eventloop.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.channels.Selector;

/**
 * Is used to replace the inefficient {@link java.util.HashSet} in {@link sun.nio.ch.SelectorImpl}
 * into {@link OptimizedSelectedKeysSet}. Replace fields to advance performance for the {@link Selector}
 */
public final class Utils {
	private static final Logger logger = LoggerFactory.getLogger(Utils.class);

	private static Field SELECTED_KEYS_FIELD;
	private static Field PUBLIC_SELECTED_KEYS_FIELD;


	static {
		try {
			Class<?> cls = Class.forName("sun.nio.ch.SelectorImpl", false, ClassLoader.getSystemClassLoader());
			SELECTED_KEYS_FIELD = cls.getDeclaredField("selectedKeys");
			PUBLIC_SELECTED_KEYS_FIELD = cls.getDeclaredField("publicSelectedKeys");
			SELECTED_KEYS_FIELD.setAccessible(true);
			PUBLIC_SELECTED_KEYS_FIELD.setAccessible(true);
		} catch (ClassNotFoundException | NoSuchFieldException e) {
			logger.warn("Failed reflecting NIO selector fields", e);
		}
	}

	/**
	 * Replaces the selected keys field from {@link java.util.HashSet} in {@link sun.nio.ch.SelectorImpl}
	 * to {@link OptimizedSelectedKeysSet} to avoid overhead which causes a work of GC
	 *
	 * @param selector selector instance whose selected keys field is to be changed
	 * @return <code>true</code> on success
	 */
	public static boolean tryToOptimizeSelector(Selector selector) {
		OptimizedSelectedKeysSet selectedKeys = new OptimizedSelectedKeysSet();
		try {
			SELECTED_KEYS_FIELD.set(selector, selectedKeys);
			PUBLIC_SELECTED_KEYS_FIELD.set(selector, selectedKeys);
			return true;

		} catch (IllegalAccessException e) {
			logger.warn("Failed setting optimized set into selector", e);
		}

		return false;
	}
}
