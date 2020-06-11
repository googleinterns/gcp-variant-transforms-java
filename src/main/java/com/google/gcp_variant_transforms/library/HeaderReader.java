// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.common.collect.ImmutableList;
import java.io.IOException;

/** Service for reading Header Lines from VCF files. */
public interface HeaderReader {
  /**
   * Parses VCF files and return raw header lines.
   *
   * @return {@link ImmutableList} of header rows.
   * @throws IOException
   */
  public abstract ImmutableList<String> getHeaderLines() throws IOException;
}
