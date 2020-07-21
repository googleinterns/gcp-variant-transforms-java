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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility constants and methods for write values with defined value type into Big Query rows.
 */
public class VariantToBqUtils {

  private static final String MISSING_FIELD_VALUE = ".";
  private static final String PHASESET_FORMAT_KEY = "PS";
  private static final String DEFAULT_PHASESET = "*";
  private static final int MISSING_GENOTYPE_VALUE = -1;

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
  }

  public static String buildReferenceBase(VariantContext variantContext) {
    Allele referenceAllele = variantContext.getReference();
    // If the ref length is longer than 1, store the ref as a String
    return referenceAllele.getDisplayString();
  }

  public static List<TableRow> addAlternates(VariantContext variantContext) {
    List<TableRow> altMetadata = new ArrayList<>();
    List<Allele> altAlleles = variantContext.getAlternateAlleles();
    for (Allele allele : altAlleles) {
      TableRow subRow = new TableRow();
      String altBase = allele.getDisplayString();
      if (altBase.length() == 0) {
        subRow.set(ColumnKeyConstants.ALTERNATE_BASES_ALT, null);
      } else {
        subRow.set(ColumnKeyConstants.ALTERNATE_BASES_ALT, altBase);
      }
      altMetadata.add(subRow);
    }
    return altMetadata;
  }

  /**
   * Add variant Info field into BQ table row.
   * If the info field number in the VCF header is `A`, add it to the ALT sub field.
   */
  public static void addInfo(TableRow row, VariantContext variantContext,
                             List<TableRow> altMetadata, VCFHeader vcfHeader) {
    Map<String, Object> info = new HashMap<>();
    for (Map.Entry<String, Object> entry : variantContext.getAttributes().entrySet()) {
      String attrName = entry.getKey();
      Object value = entry.getValue();
      VCFInfoHeaderLine infoMetadata = vcfHeader.getInfoHeaderLine(attrName);
      VCFHeaderLineType infoType = infoMetadata.getType();
      VCFHeaderLineCount infoCount = infoMetadata.getCountType();
      if (infoCount == VCFHeaderLineCount.A) {
        // put this info into ALT field
        splitAlternateAlleleInfoFields(attrName, value, altMetadata, infoType);
      } else {
        row.set(attrName, convertToDefinedType(value, infoType));
      }
    }
  }

  public static List<TableRow> addCalls(GenotypesContext genotypes, VCFHeader vcfHeader) {
    List<TableRow> callRows = new ArrayList<>();
    for (Genotype genotype : genotypes) {
      TableRow curRow = new TableRow();
      curRow.set(ColumnKeyConstants.CALLS_NAME, genotype.getSampleName());
      addInfoAndPhaseSet(curRow, genotype, vcfHeader);
      addGenotypes(curRow, genotype.getAlleles());
      callRows.add(curRow);
    }
    return callRows;
  }

  /**
   * Convert the value to the type defined in the metadata
   * If the value is an instance of List, iterate the list
   * and call convertSingleObjectToDefinedType() to convert every value
   */
  public static Object convertToDefinedType(Object value, VCFHeaderLineType type) {
    if (!(value instanceof List)) {
      // deal with single value
      // There are some field value is a single String with ',', eg: "HQ" ->"23,27"
      // We need to convert it to list of String and call the convert function again
      if ((value instanceof String) && ((String)value).contains(",")) {
        String valueStr = (String)value;
        return convertToDefinedType(Arrays.asList(valueStr.split(",")), type);
      }
      return convertSingleObjectToDefinedType(value, type);
    } else {
      // deal with list of values
      List<Object> valueList = (List<Object>)value;
      List<Object> convertedList = new ArrayList<>();
      for (Object val : valueList) {
        convertedList.add(convertSingleObjectToDefinedType(val, type));
      }
      return convertedList;
    }
  }
  /**
   * For a single value Object, convert to the type defined in the metadata
   */
  private static Object convertSingleObjectToDefinedType(Object value, VCFHeaderLineType type) {
    // values and their types have already been parsed by VCFCodec.decode()
    if (!(value instanceof String)) {
      return value;
    }

    String valueStr = (String)value;
    valueStr = valueStr.trim();   // for cases like " 27", ignore the space in list element

    if (valueStr.equals(MISSING_FIELD_VALUE)) {
      return null;
    }

    // double check if the String value match the type and also is a number
    boolean isNumber = valueStr.matches("[-+]?\\d+(\\.\\d+)?");

    if (type == VCFHeaderLineType.Integer && isNumber) {
      return Integer.parseInt(valueStr);
    } else if (type == VCFHeaderLineType.Float && isNumber) {
      return Double.parseDouble(valueStr);
    } else {
      // default String value
      return value;
    }
  }

  private static void addGenotypes(TableRow row, List<Allele> alleles) {
    List<Integer> genotypes = new ArrayList<>();
    for (Allele allele : alleles) {
      byte[] bases = allele.getDisplayBases();
      if (bases.length == 0) {
        // MISSING_VALUE is presented, should add -1
        genotypes.add(MISSING_GENOTYPE_VALUE);
      } else {
        for (byte base : bases) {
          genotypes.add((int)base);
        }
      }
    }
    row.set(ColumnKeyConstants.CALLS_GENOTYPE, genotypes);
  }

  private static void addInfoAndPhaseSet(TableRow row, Genotype genotype, VCFHeader vcfHeader) {
    String phaseSet = "";

    if (genotype.hasAD()) { row.set("AD", genotype.getAD()); }
    if (genotype.hasDP()) { row.set("DP", genotype.getDP()); }
    if (genotype.hasGQ()) { row.set("GQ", genotype.getGQ()); }
    if (genotype.hasPL()) { row.set("PL", genotype.getPL()); }
    Map<String, Object> extendedAttributes = genotype.getExtendedAttributes();
    for (String extendedAttr : extendedAttributes.keySet()) {
      if (extendedAttr.equals(PHASESET_FORMAT_KEY)) {
        String phaseSetValue = extendedAttributes.get(PHASESET_FORMAT_KEY).toString();
        phaseSet = dealWithMissingFieldValue(phaseSetValue);
      } else {
        // The rest of fields need to be converted to the right type by VCFCodec decode function
        row.set(extendedAttr, convertToDefinedType(extendedAttributes.get(extendedAttr),
                vcfHeader.getFormatHeaderLine(extendedAttr).getType()));
      }
    }
    if (phaseSet == null || phaseSet.length() != 0) {
      // phaseSet is presented(MISSING_FIELD_VALUE('.') or real value)
      row.set(ColumnKeyConstants.CALLS_PHASESET, phaseSet);
    } else {
      row.set(ColumnKeyConstants.CALLS_PHASESET, DEFAULT_PHASESET);
    }
  }


  /**
   * In some fields that values are MISSING_FIELD_VALUE('.'), should set null in BQ row.
   */
  private static String dealWithMissingFieldValue(String value) {
    return value.equals(MISSING_FIELD_VALUE) ? null : value;
  }

  private static void splitAlternateAlleleInfoFields(String attrName, Object value,
                                                     List<TableRow> altMetadata,
                                                     VCFHeaderLineType type) {
    if (value instanceof List && altMetadata.size() > 1) {
      // The alt field element size > 1, thus the value should be list and the size should be equal
      // to the alt field size
      List<Object> valList = (List<Object>)value;
      for (int i = 0; i < altMetadata.size(); ++i) {
        altMetadata.get(i).set(attrName, convertSingleObjectToDefinedType(valList.get(i), type));
      }
    } else {
      // The alt field element size == 1, thus the value should be a single list
      altMetadata.get(0).set(attrName, convertSingleObjectToDefinedType(value, type));
    }
  }
}
