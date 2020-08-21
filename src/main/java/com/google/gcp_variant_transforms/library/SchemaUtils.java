// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.gcp_variant_transforms.common.Constants;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility constants and maps for creating the Table Schema
 */
public class SchemaUtils {

  public static class FieldIndex {
    public static final int REFERENCE_NAME = 0;
    public static final int START_POSITION = 1;
    public static final int END_POSITION = 2;
    public static final int REFERENCE_BASES = 3;
    public static final int NAMES = 4;
    public static final int QUALITY = 5;
    public static final int FILTER = 6;
    public static final int CALLS = 7;

    // Indices of the Call sub-Field list.
    public static final int CALLS_SAMPLE_NAME = 0;
    public static final int CALLS_GENOTYPE = 1;
    public static final int CALLS_PHASESET = 2;
  }

  public static class FieldDescription {
    public static final String REFERENCE_NAME = "Reference name.";
    public static final String START_POSITION = "Start position (1-based). Corresponds to the first " +
        "base of the string of reference bases.";
    public static final String END_POSITION = "End position. Corresponds to the first base " +
        "after the last base in the reference allele.";
    public static final String REFERENCE_BASES = "Reference bases.";
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

  public static class FieldMode {
    public static final String NULLABLE = "NULLABLE";
    public static final String REQUIRED = "REQUIRED";
    public static final String REPEATED = "REPEATED";
  }

  public static class FieldType {
    // This list of types is not exhaustive; other possible TableFieldSchema types exist.
    public static final String INTEGER = "INT64"; // Same as "INTEGER"
    public static final String FLOAT = "FLOAT64"; // Same as "FLOAT"
    public static final String BOOLEAN = "BOOLEAN"; // Same as "BOOL"
    public static final String RECORD = "RECORD"; // Same as "STRUCT"
    public static final String STRING = "STRING";
  }

  // Maps a constant Field name to its description.
  public static Map<String, String> constantFieldNameToDescriptionMap = Stream.of(new String[][]{
      {Constants.ColumnKeyNames.REFERENCE_NAME, FieldDescription.REFERENCE_NAME},
      {Constants.ColumnKeyNames.START_POSITION, FieldDescription.START_POSITION},
      {Constants.ColumnKeyNames.END_POSITION, FieldDescription.END_POSITION},
      {Constants.ColumnKeyNames.REFERENCE_BASES, FieldDescription.REFERENCE_BASES},
      {Constants.ColumnKeyNames.NAMES, FieldDescription.NAMES},
      {Constants.ColumnKeyNames.QUALITY, FieldDescription.QUALITY},
      {Constants.ColumnKeyNames.FILTER, FieldDescription.FILTER},
      {Constants.ColumnKeyNames.CALLS, FieldDescription.CALLS},
      }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a constant Field name to its mode.
  public static Map<String, String> constantFieldNameToModeMap = Stream.of(new String[][]{
      {Constants.ColumnKeyNames.REFERENCE_NAME, FieldMode.NULLABLE},
      {Constants.ColumnKeyNames.START_POSITION, FieldMode.NULLABLE},
      {Constants.ColumnKeyNames.END_POSITION, FieldMode.NULLABLE},
      {Constants.ColumnKeyNames.REFERENCE_BASES, FieldMode.NULLABLE},
      {Constants.ColumnKeyNames.NAMES, FieldMode.REPEATED},
      {Constants.ColumnKeyNames.QUALITY, FieldMode.NULLABLE},
      {Constants.ColumnKeyNames.FILTER, FieldMode.REPEATED},
      {Constants.ColumnKeyNames.CALLS, FieldMode.REPEATED},
      }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a constant Field name to its type.
  public static Map<String, String> constantFieldNameToTypeMap = Stream.of(new String[][]{
      {Constants.ColumnKeyNames.REFERENCE_NAME, FieldType.STRING},
      {Constants.ColumnKeyNames.START_POSITION, FieldType.INTEGER},
      {Constants.ColumnKeyNames.END_POSITION, FieldType.INTEGER},
      {Constants.ColumnKeyNames.REFERENCE_BASES, FieldType.STRING},
      {Constants.ColumnKeyNames.NAMES, FieldType.STRING},
      {Constants.ColumnKeyNames.QUALITY, FieldType.FLOAT},
      {Constants.ColumnKeyNames.FILTER, FieldType.STRING},
      {Constants.ColumnKeyNames.CALLS, FieldType.RECORD},
      }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a Call sub-field to its type.
  public static Map<String, String> callSubFieldNameToTypeMap = Stream.of(new String[][]{
      {Constants.ColumnKeyNames.CALLS_PHASESET, FieldType.STRING},
      {Constants.ColumnKeyNames.CALLS_GENOTYPE, FieldType.INTEGER},
      {Constants.ColumnKeyNames.CALLS_SAMPLE_NAME, FieldType.STRING}, //verify
      }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a Call sub-fields to its mode.
  public static Map<String, String> callSubFieldNameToModeMap = Stream.of(new String[][]{
      {Constants.ColumnKeyNames.CALLS_PHASESET, FieldMode.NULLABLE},
      {Constants.ColumnKeyNames.CALLS_GENOTYPE, FieldMode.REPEATED},
      {Constants.ColumnKeyNames.CALLS_SAMPLE_NAME, FieldMode.NULLABLE},
      }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a Call sub-fields to its description.
  public static Map<String, String> callSubFieldNameToDescriptionMap = Stream.of(new String[][]{
      {Constants.ColumnKeyNames.CALLS_PHASESET, FieldDescription.CALLS_PHASESET},
      {Constants.ColumnKeyNames.CALLS_GENOTYPE, FieldDescription.CALLS_GENOTYPE},
      {Constants.ColumnKeyNames.CALLS_SAMPLE_NAME, FieldDescription.CALLS_SAMPLE_NAME},
      }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a constant Field index to its name with a TreeMap, ordered by field index.
  public static Map<Integer, String> constantFieldIndexToNameMap = new TreeMap<Integer, String>() {{
      put(FieldIndex.REFERENCE_NAME, Constants.ColumnKeyNames.REFERENCE_NAME);
      put(FieldIndex.START_POSITION, Constants.ColumnKeyNames.START_POSITION);
      put(FieldIndex.END_POSITION, Constants.ColumnKeyNames.END_POSITION);
      put(FieldIndex.REFERENCE_BASES, Constants.ColumnKeyNames.REFERENCE_BASES);
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
}
