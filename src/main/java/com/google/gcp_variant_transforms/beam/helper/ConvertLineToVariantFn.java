// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.beam.helper;

import com.google.common.collect.ImmutableList;
import com.google.gcp_variant_transforms.entity.Variant;
import com.google.gcp_variant_transforms.library.VcfParser;
import htsjdk.variant.vcf.VCFCodec;
import org.apache.beam.sdk.transforms.DoFn;

/** {@link DoFn} implementation for decoding variant VCF lines into Variant objects. */
public class ConvertLineToVariantFn extends DoFn<String, Variant> {
  private static final long serialVersionUID = -5248117228853194645L;
  private final VcfParser vcfParser;
  private final ImmutableList<String> headerLines;
  private VCFCodec vcfCodec = null;

  public ConvertLineToVariantFn(VcfParser vcfParser, ImmutableList<String> headerLines) {
    this.vcfParser = vcfParser;
    this.headerLines = headerLines;
  }

  @ProcessElement
  public void processElement(@Element String record, OutputReceiver<Variant> receiver) {
    // Codec's are not Serializable, so they need to be generated directly on the worker machine.
    if (vcfCodec == null) {
      vcfCodec = vcfParser.generateCodecFromHeaderLines(headerLines);
    }
    receiver.output(new Variant(vcfCodec.decode(record)));
  }
}
