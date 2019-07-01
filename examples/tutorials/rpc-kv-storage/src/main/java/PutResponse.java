import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.serializer.annotations.SerializeNullable;

// [START EXAMPLE]
public class PutResponse {
	private final String previousValue;

	public PutResponse(@Deserialize("previousValue") String previousValue) {
		this.previousValue = previousValue;
	}

	@Serialize(order = 0)
	@SerializeNullable
	public String getPreviousValue() {
		return previousValue;
	}

	@Override
	public String toString() {
		return "{previousValue='" + previousValue + '\'' + '}';
	}
}
// [END EXAMPLE]
