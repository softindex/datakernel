package io.datakernel.http;

public interface HttpExceptionFormatter {
	HttpResponse formatException(Exception e);
}
