package by.webapp.kvstorage.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
public class FailedException extends RuntimeException {
    private static String message = "Operation failed.";

    public FailedException() {
        super(message);
    }

    public FailedException(String message) {
        super(message);
    }

    public FailedException(String message, Throwable cause) {
        super(message, cause);
    }

}
