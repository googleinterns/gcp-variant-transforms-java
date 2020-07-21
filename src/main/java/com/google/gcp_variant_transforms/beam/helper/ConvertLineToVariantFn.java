// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.beam.helper;

import com.google.common.collect.ImmutableList;
import com.google.gcp_variant_transforms.library.VcfParser;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.beam.sdk.transforms.DoFn;

/** {@link DoFn} implementation for decoding variant VCF lines into Variant objects. */
public class ConvertLineToVariantFn extends DoFn<String, VariantContext> {
  private static final long serialVersionUID = -5248117228853194645L;
  private final VcfParser vcfParser;
  private final ImmutableList<String> headerLines;
  private VCFCodec vcfCodec = null;
  private VCFHeader vcfHeader = null;

  public ConvertLineToVariantFn(VcfParser vcfParser, ImmutableList<String> headerLines) {
    this.vcfParser = vcfParser;
    this.headerLines = headerLines;
  }

  @ProcessElement
  public void processElement(@Element String record, OutputReceiver<VariantContext> receiver) {
    // Codec's are not Serializable, so they need to be generated directly on the worker machine.
    if (vcfCodec == null) {
      vcfCodec = vcfParser.generateCodecFromHeaderLines(headerLines);
      vcfHeader = vcfCodec.getHeader();
    }
    receiver.output(vcfCodec.decode(record));
  }
}
