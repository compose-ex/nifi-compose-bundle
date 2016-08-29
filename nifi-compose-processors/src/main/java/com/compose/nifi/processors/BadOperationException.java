package com.compose.nifi.processors;

/**
 * Created by hayshutton on 8/26/16.
 */
public class BadOperationException extends Exception {
  public BadOperationException(String message) {
    super(message);
  }

  public BadOperationException(Throwable throwable) {
    super(throwable);
  }

  public BadOperationException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
