package io.datakernel.remotefs;

/**
 * An exception wrapper class which carries id of the partition that caused the exception.
 */
public class PartitionException extends RemoteFsException {

	private final Object partitionId;

	public PartitionException(String message, Object partitionId, Throwable cause) {
		super(message, cause);
		this.partitionId = partitionId;
	}

	public PartitionException(Object partitionId, Throwable cause) {
		super(cause);
		this.partitionId = partitionId;
	}

	@Override
	public String toString() {
		String s = getClass().getName() + '(' + partitionId + ')';
		String message = getLocalizedMessage();
		return (message != null) ? (s + ": " + message) : s;
	}
}
