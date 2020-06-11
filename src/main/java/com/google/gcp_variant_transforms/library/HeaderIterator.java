// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.common.collect.ImmutableList;
import htsjdk.variant.vcf.VCFCodec;
import java.io.Serializable;

import htsjdk.tribble.readers.LineIterator;

/**
 * Custom Iterator for {@link VCFCodec}.
 */
public class HeaderIterator implements LineIterator, Serializable {
  private static final long serialVersionUID = 60231493676096593L;
  private final ImmutableList<String> headerLines;
  private int currentIndex;

  public HeaderIterator(ImmutableList<String> headerLines) {
    this.headerLines = headerLines;
    this.currentIndex = 0;
  }

  @Override
  public boolean hasNext() {
    return currentIndex < headerLines.size();
  }

  @Override
  public String next() {
    return headerLines.get(currentIndex++);
  }

  @Override
  public String peek() {
    return headerLines.get(currentIndex);
  }
}
