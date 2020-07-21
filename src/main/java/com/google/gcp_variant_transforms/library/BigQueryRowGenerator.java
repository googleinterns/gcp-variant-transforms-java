// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.api.services.bigquery.model.TableRow;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;

/**
 * Service to generate Big Query Row from VariantContext
 */
public interface BigQueryRowGenerator {
  /**
   * BigQuery row generator. It provides the common functionalities when generating BigQuery
   * row (e.g., sanitizing the BigQuery field, resolving the conflicts between the schema and data).
   */
  public TableRow getRows(VariantContext variantContext, VCFHeader vcfHeader);
}
