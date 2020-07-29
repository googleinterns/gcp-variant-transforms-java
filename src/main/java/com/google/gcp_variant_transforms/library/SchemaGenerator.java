// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.cloud.bigquery.Schema;
import htsjdk.variant.vcf.VCFHeader;

/**
 * Service to create BigQuery schema from VCFHeader 
 */
public interface SchemaGenerator {

  public Schema getSchema(VCFHeader vcfHeader);
}
