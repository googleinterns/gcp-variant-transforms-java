// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.common.collect.ImmutableList;
import com.google.inject.Singleton;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;

/** Implementation of {@link VcfParser} service. */
@Singleton
public class VcfParserImpl implements VcfParser {

  private static final long serialVersionUID = 6787944179556195968L;

  @Override
  public VCFCodec generateCodecFromHeaderLines(ImmutableList<String> headerLines) {
    VCFCodec vcfCodec = new VCFCodec();
    vcfCodec.readActualHeader(new HeaderIterator(headerLines));
    return vcfCodec;
  }

  @Override
  public VCFHeader generateVCFHeader(ImmutableList<String> headerLines){
    VCFCodec vcfCodec = new VCFCodec();
    vcfCodec.readActualHeader(new HeaderIterator(headerLines));
    return vcfCodec.getHeader();
  }
}
