// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.entity;

import java.io.Serializable;

/**
 * Malformed VCF record, it should include the contig, start position, reference bases and error
 * message.
 */
public class MalformedRecord implements Serializable {
  private final String referenceName;
  private final String errorMessage;
  private final String referenceBases;
  private final String start;

  public MalformedRecord(String referenceName, String start, String referenceBases,
                         String errorMessage) {
    this.referenceName = referenceName;
    this.start = start;
    this.referenceBases = referenceBases;
    this.errorMessage = errorMessage;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public String getReferenceBases() {
    return referenceBases;
  }

  public String getStart() {
    return start;
  }
}
