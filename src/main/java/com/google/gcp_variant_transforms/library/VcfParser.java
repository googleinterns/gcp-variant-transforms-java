// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.common.collect.ImmutableList;
import htsjdk.variant.vcf.VCFCodec;
import java.io.Serializable;

/**
 * Service to parsing VCF records.
 */
public interface VcfParser extends Serializable {
  /**
   * Generates a codec from the supplied header lines. This code should be used inside workers
   * since codecs are not serializable, and hence cannot be supplied to the worker
   * machines/subprocesses.
   *
   * @param headerLines to preset into codec.
   * @return {@link VCFCodec}, preset with header lines, to parse records one at a time.
   */
  public abstract VCFCodec generateCodecFromHeaderLines(ImmutableList<String> headerLines);
}
