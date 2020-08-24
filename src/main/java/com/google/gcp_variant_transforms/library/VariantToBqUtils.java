// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.api.services.bigquery.model.TableRow;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Utility constants and methods for write values with defined value type into Big Query rows.
 */
public class VariantToBqUtils {

  /**
   * Constants for column names in the BigQuery schema.
   */
  public static class ColumnKeyConstants {
    public static final String REFERENCE_NAME = "reference_name";
    public static final String START_POSITION = "start_position";
    public static final String END_POSITION = "end_position";
    public static final String REFERENCE_BASES = "reference_bases";
    public static final String ALTERNATE_BASES = "alternate_bases";
    public static final String ALTERNATE_BASES_ALT = "alt";
    public static final String NAMES = "names";
    public static final String QUALITY = "quality";
    public static final String FILTER = "filter";
    public static final String CALLS = "call";
    public static final String CALLS_NAME = "name";
    public static final String CALLS_GENOTYPE = "genotype";
    public static final String CALLS_PHASESET = "phaseset";
    public static final String CALLS_SAMPLE_ID = "sample_id";
  }
}
