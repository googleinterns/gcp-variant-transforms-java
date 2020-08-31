// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.exceptions;

/**
 * Runtime exception for VCF record value count does not match the count defined in VCFHeader.
 */
public class CountNotMatchException extends RuntimeException {
  public CountNotMatchException(String message) {
    super(message);
  }

  public CountNotMatchException(String message, Throwable cause) {
    super(message, cause);
  }

  public CountNotMatchException(Throwable cause) {
    super(cause);
  }
}
