package io.datakernel.ot.exceptions;

public class OTNoCommitException extends OTException {
	private static final String NO_COMMIT_MESSAGE = "No commit with id: ";

	public OTNoCommitException(long revisionId) {
		super(NO_COMMIT_MESSAGE + revisionId);
	}
}
