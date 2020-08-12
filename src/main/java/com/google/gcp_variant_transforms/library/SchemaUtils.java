// Copyright 2020 Google LLC

package.com.google.gcp_variant_transforms.library;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility constants for creating the Table Schema 
 */
public static class SchemaUtils {

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
    public static final String CALLS_NAME = "name";
    public static final String CALLS_GENOTYPE = "genotype";
    public static final String CALLS_PHASESET = "phaseset";
    public static final String CALLS_SAMPLE_ID = "sample_id";
  }

  public static class FieldDescription {
    public static final String REFERENCE_NAME = "Reference name.";
    public static final String START_POSITION = "Start position. Corresponds to the first " +
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
    public static final String CALLS_SAMPLE_ID = "Unique ID (type INT64) assigned to each " +
                                                "sample. Table with `__sample_info` suffix " +
                                                "contains the mapping of sample names (as read" +
                                                "from VCF header) to these assigned IDs.";
    public static final String CALLS_GENOTYPE = "Genotype of the call. \"-1\" is used in cases " +
                                                "where the genotype is not called.";
    public static final String CALLS_PHASESET = "Phaseset of the call (if any). \"*\" is used " +
                                                "in cases where the genotype is phased, but no " +
                                                "phase set (\"PS\" in FORMAT) was specified.";
  }


  public static Map<String, String> FieldNameToDescription = Stream.of(new String[][]) {
          { FieldName.REFERENCE_NAME, FieldDescription.REFERENCE_NAME },
          { FieldName.START_POSITION, FieldDescription.START_POSITION },
          { FieldName.END_POSITION, FieldDescription.END_POSITION },
          { FieldName.REFERENCE_BASES, FieldDescription.REFERENCE_BASES }, 
          { FieldName.NAMES, FieldDescription.NAMES },
          { FieldName.QUALITY, FieldDescription.QUALITY },
          { FieldName.FILTER, FieldDescription.FILTER }, 
          { FieldName.CALLS, FieldDescription.CALLS }, 
          { FieldName.CALLS_SAMPLE_ID, FieldDescription.CALLS_SAMPLE_ID },
          { FieldName.CALLS_GENOTYPE, FieldDescription.CALLS_GENOTYPE },
          { FieldName.CALLS_PHASESET, FieldDescription.CALLS_PHASESET }
      }).collect(Collectors.toMap(data -> data[0], data -> data[1]));

}