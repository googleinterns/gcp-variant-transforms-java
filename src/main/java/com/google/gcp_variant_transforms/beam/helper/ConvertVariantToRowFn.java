// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.beam.helper;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gcp_variant_transforms.library.BigQueryRowGenerator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.beam.sdk.transforms.DoFn;

/** {@link DoFn} implementation for a VariantContext to a BigQuery Row. */
public class ConvertVariantToRowFn extends DoFn<VariantContext, TableRow> {
  private final boolean allowIncompatibleRecords = false;
  private final boolean omitEmptySampleCalls = false;
  private BigQueryRowGenerator bigQueryRowGenerator;
  private VCFHeader vcfHeader;

  public ConvertVariantToRowFn(VCFHeader vcfHeader, BigQueryRowGenerator bigQueryRowGenerator) {
    this.bigQueryRowGenerator = bigQueryRowGenerator;
    this.vcfHeader = vcfHeader;
  }

  @ProcessElement
  public void processElement(@Element VariantContext variantContext, OutputReceiver<TableRow> receiver) {
    receiver.output(bigQueryRowGenerator.getRows(variantContext, vcfHeader));
  }
}
