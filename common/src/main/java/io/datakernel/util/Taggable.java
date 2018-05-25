package io.datakernel.util;

import io.datakernel.annotation.Nullable;

/**
 * This interface marks type as taggable, which means that some kind of a tag
 * could be attached to it.
 * This is mostly used to differentiate objects when logging.
 */
public interface Taggable {

	/**
	 * Returns attached tag object of any.
	 *
	 * @return attached tag object or <code>null</code>
	 */
	@Nullable
	Object getTag();

	/**
	 * Attaches some tag object to this {@link Taggable} instance.
	 *
	 * @param tag object to attach
	 */
	void setTag(@Nullable Object tag);
}
