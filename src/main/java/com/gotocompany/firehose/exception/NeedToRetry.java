package com.gotocompany.firehose.exception;

/**
 * Checked exception signalling that an operation failed with a status that falls within the
 * configured retry range and therefore should be attempted again.
 *
 * <p>It is used by sinks that classify downstream responses, for example the Elasticsearch sink, to
 * indicate that a bulk response contained failures whose status codes are deemed retryable.
 */
public class NeedToRetry extends Exception {
  /**
   * Creates the exception, embedding the offending status code in the detail message.
   *
   * @param statusCode the downstream status code that fell within the retry range
   */
  public NeedToRetry(String statusCode) {
    super(String.format("Status code fall under retry range. StatusCode: %s", statusCode));
  }
}
