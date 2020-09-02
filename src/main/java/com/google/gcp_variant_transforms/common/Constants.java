// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.common;

/**
 * Constants in Variant Transforms
 */
public class Constants {
  /**
   * Column filed name constants in the BigQuery schema.
   */
  public static class ColumnKeyNames {
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
    public static final String CALLS_SAMPLE_NAME = "name";
    public static final String CALLS_GENOTYPE = "genotype";
    public static final String CALLS_PHASESET = "phaseset";
  }

  /**
   * Commonly used VCF Format Header Line IDs
   */
  public static class VCFFormatIDNames {
    public static final String CALLS_GENOTYPE = "GT";
    public static final String CALLS_PHASESET = "PS";
  }
}
