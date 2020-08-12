// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import java.util.Map;
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

    //Indices of the Call sub-Field list
    public static final int CALLS_SAMPLE_NAME = 0;
    public static final int CALLS_GENOTYPE = 1;
    public static final int CALLS_PHASESET = 2;
  }

  public static class FieldName {
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
    public static final String CALLS_SAMPLE_NAME = "name"; // verify
    public static final String CALLS_GENOTYPE = "genotype";
    public static final String CALLS_PHASESET = "phaseset";
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
    // this list of types is not exhaustive; other possible TableFieldSchema types exist
    public static final String INTEGER = "INT64"; // same as "INTEGER"
    public static final String FLOAT = "FLOAT64"; // same as "FLOAT"
    public static final String BOOLEAN = "BOOLEAN"; // same as "BOOL"
    public static final String RECORD = "RECORD"; // same as "STRUCT"
    public static final String STRING = "STRING";
  }

  // Maps a constant Field name to its description
  public static Map<String, String> constantFieldNameToDescriptionMap = Stream.of(new String[][]{
      {FieldName.REFERENCE_NAME, FieldDescription.REFERENCE_NAME},
      {FieldName.START_POSITION, FieldDescription.START_POSITION},
      {FieldName.END_POSITION, FieldDescription.END_POSITION},
      {FieldName.REFERENCE_BASES, FieldDescription.REFERENCE_BASES},
      {FieldName.NAMES, FieldDescription.NAMES},
      {FieldName.QUALITY, FieldDescription.QUALITY},
      {FieldName.FILTER, FieldDescription.FILTER},
      {FieldName.CALLS, FieldDescription.CALLS},
      }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a constant Field name to its mode
  public static Map<String, String> constantFieldNameToModeMap = Stream.of(new String[][]{
      {FieldName.REFERENCE_NAME, FieldMode.NULLABLE},
      {FieldName.START_POSITION, FieldMode.NULLABLE},
      {FieldName.END_POSITION, FieldMode.NULLABLE},
      {FieldName.REFERENCE_BASES, FieldMode.NULLABLE},
      {FieldName.NAMES, FieldMode.REPEATED},
      {FieldName.QUALITY, FieldMode.NULLABLE},
      {FieldName.FILTER, FieldMode.REPEATED},
      {FieldName.CALLS, FieldMode.REPEATED},
      }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a constant Field name to its type
  public static Map<String, String> constantFieldNameToTypeMap = Stream.of(new String[][]{
      {FieldName.REFERENCE_NAME, FieldType.STRING},
      {FieldName.START_POSITION, FieldType.INTEGER},
      {FieldName.END_POSITION, FieldType.INTEGER},
      {FieldName.REFERENCE_BASES, FieldType.STRING},
      {FieldName.NAMES, FieldType.STRING},
      {FieldName.QUALITY, FieldType.FLOAT},
      {FieldName.FILTER, FieldType.STRING},
      {FieldName.CALLS, FieldType.RECORD},
      }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a Call sub-field to its type
  public static Map<String, String> callSubFieldNameToTypeMap = Stream.of(new String[][]{
      {FieldName.CALLS_PHASESET, FieldType.STRING},
      {FieldName.CALLS_GENOTYPE, FieldType.INTEGER},
      {FieldName.CALLS_SAMPLE_NAME, FieldType.STRING}, //verify
      }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a Call sub-fields to its mode
  public static Map<String, String> callSubFieldNameToModeMap = Stream.of(new String[][]{
      {FieldName.CALLS_PHASESET, FieldMode.NULLABLE},
      {FieldName.CALLS_GENOTYPE, FieldMode.REPEATED},
      {FieldName.CALLS_SAMPLE_NAME, FieldMode.NULLABLE},
      }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a Call sub-fields to its description
  public static Map<String, String> callSubFieldNameToDescriptionMap = Stream.of(new String[][]{
      {FieldName.CALLS_PHASESET, FieldDescription.CALLS_PHASESET},
      {FieldName.CALLS_GENOTYPE, FieldDescription.CALLS_GENOTYPE},
      {FieldName.CALLS_SAMPLE_NAME, FieldDescription.CALLS_SAMPLE_NAME},
      }).collect(Collectors.toMap(mapData -> mapData[0], mapData -> mapData[1]));

  // Maps a constant Field index to its name
  public static Map<Integer, String> constantFieldIndexToNameMap = Stream.of(new Object[][]{
      {FieldIndex.REFERENCE_NAME, FieldName.REFERENCE_NAME},
      {FieldIndex.START_POSITION, FieldName.START_POSITION},
      {FieldIndex.END_POSITION, FieldName.END_POSITION},
      {FieldIndex.REFERENCE_BASES, FieldName.REFERENCE_BASES},
      {FieldIndex.NAMES, FieldName.NAMES},
      {FieldIndex.QUALITY, FieldName.QUALITY},
      {FieldIndex.FILTER, FieldName.FILTER},
      {FieldIndex.CALLS, FieldName.CALLS},
      }).collect(Collectors.toMap(mapData -> (Integer) mapData[0], mapData -> (String) mapData[1]));

  // Maps a Call Sub-Field index to its name
  public static Map<Integer, String> callsSubFieldIndexToNameMap = Stream.of(new Object[][]{
      {FieldIndex.CALLS_SAMPLE_NAME, FieldName.CALLS_SAMPLE_NAME},
      {FieldIndex.CALLS_GENOTYPE, FieldName.CALLS_GENOTYPE},
      {FieldIndex.CALLS_PHASESET, FieldName.CALLS_PHASESET},
      }).collect(Collectors.toMap(mapData -> (Integer) mapData[0], mapData -> (String) mapData[1]));
}
