// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.exceptions;

/**
 * Runtime Exception of malformed records. It contains the basic information of the record and
 * error message.
 */
public class MalformedRecordException extends RuntimeException {
  private final String referenceName;
  private final String start;
  private final String referenceBases;

  public MalformedRecordException(String message, String referenceName, String start,
                                  String referenceBases) {
    super(message);
    this.referenceName = referenceName;
    this.start = start;
    this.referenceBases = referenceBases;
  }

  public MalformedRecordException(String message, Throwable cause, String referenceName,
                                  String start, String referenceBases) {
    super(message, cause);
    this.referenceName = referenceName;
    this.start = start;
    this.referenceBases = referenceBases;
  }

  public MalformedRecordException(Throwable cause, String referenceName, String start,
                                  String referenceBases) {
    super(cause);
    this.referenceName = referenceName;
    this.start = start;
    this.referenceBases = referenceBases;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getStart() {
    return start;
  }

  public String getReferenceBases() {
    return referenceBases;
  }
}
