// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.api.services.bigquery.model.TableSchema;
import htsjdk.variant.vcf.VCFHeader;

/**
 * Service to create BigQuery schema from VCFHeader 
 */
public interface SchemaGenerator {

  /**
   * Creates and returns a BigQuery TableSchema from a VCF file's header
   * @param vcfHeader
   * @return TableSchema
   */
  TableSchema getSchema(VCFHeader vcfHeader);
}
