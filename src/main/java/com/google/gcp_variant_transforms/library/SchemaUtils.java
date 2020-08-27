// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.gcp_variant_transforms.common.Constants;
import htsjdk.variant.vcf.VCFHeaderLineType;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility constants and maps for creating the Table Schema
 */
public class SchemaUtils {

  // Prefix for BQ field names that do not start with alpha characters
  public static String FALLBACK_FIELD_NAME_PREFIX = "field_";

  public static class FieldIndex {
    public static final int REFERENCE_NAME = 0;
    public static final int START_POSITION = 1;
    public static final int END_POSITION = 2;
    public static final int REFERENCE_BASES = 3;
    public static final int ALTERNATE_BASES = 4;
    public static final int ALTERNATE_BASES_ALT = 0; // Field under ALTERNATE_BASES record.
    public static final int NAMES = 5;
    public static final int QUALITY = 6;
    public static final int FILTER = 7;
    public static final int CALLS = 8;

    // Indices of the Call sub-Field list.
    public static final int CALLS_SAMPLE_NAME = 0;
    public static final int CALLS_GENOTYPE = 1;
    public static final int CALLS_PHASESET = 2;
  }

  public static class FieldDescription {
    public static final String REFERENCE_NAME = "Reference name.";
    public static final String START_POSITION = "Start position (1-based). Corresponds to the " +
        "first base of the string of reference bases.";
    public static final String END_POSITION = "End position. Corresponds to the first base " +
        "after the last base in the reference allele.";
    public static final String REFERENCE_BASES = "Reference bases.";
    public static final String ALTERNATE_BASES = "One record for each alternate base (if any).";
    public static final String ALTERNATE_BASES_ALT = "Alternate base.";
    public static final String NAMES = "Variant names (e.g. RefSNP ID).";
    public static final String QUALITY = "Phred-scaled quality score (-10log10 prob(call " +
        "is wrong)). Higher values imply better quality.";
    public static final String FILTER = "List of failed filters (if any) or \"PASS\" " +
        "indicating the variant has passed all filters.";
    public static final String CALLS = "One record for each call.";
    public static final String CALLS_SAMPLE_NAME = "Name of the call.";
    public static final String CALLS_GENOTYPE = "Genotype of the call. \"-1\" is used in cases " +
        "where the genotype is not called.";
    public static final String CALLS_PHASESET = "Phaseset of the call (if any). \"*\" is used " +
        "in cases where the genotype is phased, but no " +
        "phase set (\"PS\" in FORMAT) was specified.";
  }

  public static class BQFieldMode {
    public static final String NULLABLE = "NULLABLE";
    public static final String REQUIRED = "REQUIRED";
    public static final String REPEATED = "REPEATED";
  }

  public static class BQFieldType {
    // This list of types is not exhaustive; other possible TableFieldSchema types exist.
    public static final String INTEGER = "INT64"; // Same as "INTEGER"
    public static final String FLOAT = "FLOAT64"; // Same as "FLOAT"
    public static final String BOOLEAN = "BOOLEAN"; // Same as "BOOL"
    public static final String RECORD = "RECORD"; // Same as "STRUCT"
    public static final String STRING = "STRING";
  }

  // Maps a constant Field index to its name with a TreeMap, ordered by field index.
  public static Map<Integer, String> constantFieldIndexToNameMap = new TreeMap<Integer, String>() {{
    put(FieldIndex.REFERENCE_NAME, Constants.ColumnKeyNames.REFERENCE_NAME);
    put(FieldIndex.START_POSITION, Constants.ColumnKeyNames.START_POSITION);
    put(FieldIndex.END_POSITION, Constants.ColumnKeyNames.END_POSITION);
    put(FieldIndex.REFERENCE_BASES, Constants.ColumnKeyNames.REFERENCE_BASES);
    put(FieldIndex.ALTERNATE_BASES, Constants.ColumnKeyNames.ALTERNATE_BASES);
    put(FieldIndex.NAMES, Constants.ColumnKeyNames.NAMES);
    put(FieldIndex.QUALITY, Constants.ColumnKeyNames.QUALITY);
    put(FieldIndex.FILTER, Constants.ColumnKeyNames.FILTER);
    put(FieldIndex.CALLS, Constants.ColumnKeyNames.CALLS);
  }};

  // Maps a Call Sub-Field index to its name with a TreeMap, ordered by field index.
  public static Map<Integer, String> callsSubFieldIndexToNameMap = new TreeMap<Integer, String>() {{
    put(FieldIndex.CALLS_SAMPLE_NAME, Constants.ColumnKeyNames.CALLS_SAMPLE_NAME);
    put(FieldIndex.CALLS_GENOTYPE, Constants.ColumnKeyNames.CALLS_GENOTYPE);
    put(FieldIndex.CALLS_PHASESET, Constants.ColumnKeyNames.CALLS_PHASESET);
  }};

  // Maps a constant Field name to its description.
  public static Map<String, String> constantFieldNameToDescriptionMap = Stream.of(new String[][] {
      {Constants.ColumnKeyNames.REFERENCE_NAME, FieldDescription.REFERENCE_NAME},
      {Constants.ColumnKeyNames.START_POSITION, FieldDescription.START_POSITION},
      {Constants.ColumnKeyNames.END_POSITION, FieldDescription.END_POSITION},
      {Constants.ColumnKeyNames.REFERENCE_BASES, FieldDescription.REFERENCE_BASES},
      {Constants.ColumnKeyNames.ALTERNATE_BASES, FieldDescription.ALTERNATE_BASES},
      {Constants.ColumnKeyNames.NAMES, FieldDescription.NAMES},
      {Constants.ColumnKeyNames.QUALITY, FieldDescription.QUALITY},
      {Constants.ColumnKeyNames.FILTER, FieldDescription.FILTER},
      {Constants.ColumnKeyNames.CALLS, FieldDescription.CALLS},
      }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a Call sub-field to its description.
  public static Map<String, String> callSubFieldNameToDescriptionMap = Stream.of(new String[][] {
      {Constants.ColumnKeyNames.CALLS_PHASESET, FieldDescription.CALLS_PHASESET},
      {Constants.ColumnKeyNames.CALLS_GENOTYPE, FieldDescription.CALLS_GENOTYPE},
      {Constants.ColumnKeyNames.CALLS_SAMPLE_NAME, FieldDescription.CALLS_SAMPLE_NAME},
  }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a constant Field name to its mode.
  public static Map<String, String> constantFieldNameToModeMap = Stream.of(new String[][] {
      {Constants.ColumnKeyNames.REFERENCE_NAME, BQFieldMode.NULLABLE},
      {Constants.ColumnKeyNames.START_POSITION, BQFieldMode.NULLABLE},
      {Constants.ColumnKeyNames.END_POSITION, BQFieldMode.NULLABLE},
      {Constants.ColumnKeyNames.REFERENCE_BASES, BQFieldMode.NULLABLE},
      {Constants.ColumnKeyNames.ALTERNATE_BASES, BQFieldMode.REPEATED},
      {Constants.ColumnKeyNames.NAMES, BQFieldMode.REPEATED},
      {Constants.ColumnKeyNames.QUALITY, BQFieldMode.NULLABLE},
      {Constants.ColumnKeyNames.FILTER, BQFieldMode.REPEATED},
      {Constants.ColumnKeyNames.CALLS, BQFieldMode.REPEATED},
      }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a Call sub-fields to its mode.
  public static Map<String, String> callSubFieldNameToModeMap = Stream.of(new String[][] {
      {Constants.ColumnKeyNames.CALLS_PHASESET, BQFieldMode.NULLABLE},
      {Constants.ColumnKeyNames.CALLS_GENOTYPE, BQFieldMode.REPEATED},
      {Constants.ColumnKeyNames.CALLS_SAMPLE_NAME, BQFieldMode.NULLABLE},
  }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a constant Field name to its type.
  public static Map<String, String> constantFieldNameToTypeMap = Stream.of(new String[][] {
      {Constants.ColumnKeyNames.REFERENCE_NAME, BQFieldType.STRING},
      {Constants.ColumnKeyNames.START_POSITION, BQFieldType.INTEGER},
      {Constants.ColumnKeyNames.END_POSITION, BQFieldType.INTEGER},
      {Constants.ColumnKeyNames.REFERENCE_BASES, BQFieldType.STRING},
      {Constants.ColumnKeyNames.ALTERNATE_BASES, BQFieldType.RECORD},
      {Constants.ColumnKeyNames.NAMES, BQFieldType.STRING},
      {Constants.ColumnKeyNames.QUALITY, BQFieldType.FLOAT},
      {Constants.ColumnKeyNames.FILTER, BQFieldType.STRING},
      {Constants.ColumnKeyNames.CALLS, BQFieldType.RECORD},
      }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a Call sub-field to its type.
  public static Map<String, String> callSubFieldNameToTypeMap = Stream.of(new String[][] {
      {Constants.ColumnKeyNames.CALLS_PHASESET, BQFieldType.STRING},
      {Constants.ColumnKeyNames.CALLS_GENOTYPE, BQFieldType.INTEGER},
      {Constants.ColumnKeyNames.CALLS_SAMPLE_NAME, BQFieldType.STRING}, //verify
      }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps the HTSJDK HeaderLine types to BQ Field types
  public static Map<VCFHeaderLineType, String> HTSJDKTypeToBQTypeMap =
      new HashMap<VCFHeaderLineType, String>() {{
          put(VCFHeaderLineType.String, BQFieldType.STRING);
          put(VCFHeaderLineType.Float, BQFieldType.FLOAT);
          put(VCFHeaderLineType.Integer, BQFieldType.INTEGER);
  }};
}
