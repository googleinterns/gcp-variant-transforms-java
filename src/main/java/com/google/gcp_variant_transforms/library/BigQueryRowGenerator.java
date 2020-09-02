// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.api.services.bigquery.model.TableRow;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;

/**
 * Service to generate Big Query Row from VariantContext. It provides functionalities when
 * generating BigQuery row.
 */
public interface BigQueryRowGenerator {

  /**
   * Convert each field value in {@link VariantContext} to the format defined by {@link VCFHeader},
   * and set each field in a BQ {@link TableRow}.
   * @param variantContext
   * @param vcfHeader
   * @return TableRow with all field values of one VCF record.
   */
  public TableRow convertToBQRow(VariantContext variantContext, VCFHeader vcfHeader);
}
