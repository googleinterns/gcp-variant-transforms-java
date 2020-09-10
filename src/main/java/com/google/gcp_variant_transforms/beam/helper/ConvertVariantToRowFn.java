// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.beam.helper;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gcp_variant_transforms.library.BigQueryRowGenerator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

/** {@link DoFn} implementation for a VariantContext to a BigQuery Row. */
public class ConvertVariantToRowFn extends DoFn<VariantContext, TableRow> {

  private final boolean allowMalformedRecords;
  private final boolean useOneBasedCoordinate;
  private final BigQueryRowGenerator bigQueryRowGenerator;
  private final VCFHeader vcfHeader;
  private final TupleTag<TableRow> validRecords;
  private final TupleTag<String> errorMessages;

  public ConvertVariantToRowFn(BigQueryRowGenerator bigQueryRowGenerator, VCFHeader vcfHeader,
                               boolean allowMalformedRecords, boolean useOneBasedCoordinate,
                               TupleTag<TableRow> validRecords, TupleTag<String> errorMessages) {
    this.bigQueryRowGenerator = bigQueryRowGenerator;
    this.vcfHeader = vcfHeader;
    this.allowMalformedRecords = allowMalformedRecords;
    this.useOneBasedCoordinate = useOneBasedCoordinate;
    this.validRecords = validRecords;
    this.errorMessages = errorMessages;
  }

  @ProcessElement
  public void processElement(@Element VariantContext variantContext,
                             MultiOutputReceiver receiver) {
    try {
      receiver.get(validRecords)
          .output(bigQueryRowGenerator.convertToBQRow(variantContext, vcfHeader,
              useOneBasedCoordinate));
    } catch (Exception e) {
      if (allowMalformedRecords) {
        receiver.get(errorMessages).output(e.getMessage());
      } else {
        throw new RuntimeException(e.getMessage());
      }
    }
  }
}
