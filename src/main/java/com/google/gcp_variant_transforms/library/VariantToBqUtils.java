// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gcp_variant_transforms.common.Constants;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility methods for write values with defined value type into Big Query rows.
 */
public class VariantToBqUtils {
  public static String getReferenceBases(VariantContext variantContext) {
    Allele referenceAllele = variantContext.getReference();
    // If the ref length is longer than 1, store the ref as a String.
    String referenceBase = referenceAllele.getDisplayString();
    return referenceBase.equals(Constants.MISSING_FIELD_VALUE) ? null : referenceBase;
  }

  public static String getNames(VariantContext variantContext) {
    // If the ID field is ".", should write null into BQ row.
    String name = variantContext.getID();
    return name.equals(Constants.MISSING_FIELD_VALUE) ? null : name;
  }

  public static List<TableRow> getAlternateBases(VariantContext variantContext) {
    List<TableRow> altMetadata = new ArrayList<>();
    List<Allele> altAlleles = variantContext.getAlternateAlleles();
    // If field value is '.', alternateAllelesList will be empty.
    if (altAlleles.isEmpty()) {
      TableRow subRow = new TableRow();
      subRow.set(Constants.ColumnKeyConstants.ALTERNATE_BASES_ALT, null);
      altMetadata.add(subRow);
    } else {
      for (Allele altAllele : altAlleles) {
        TableRow subRow = new TableRow();
        String altBase = altAllele.getDisplayString();
        subRow.set(Constants.ColumnKeyConstants.ALTERNATE_BASES_ALT, altBase);
        altMetadata.add(subRow);
      }
    }
    return altMetadata;
  }

  public static Set<String> getFilters(VariantContext variantContext) {
    Set<String> filters = variantContext.getFilters();
    if (filters.isEmpty()) {
      filters = new HashSet<>();
      filters.add("PASS");
    }
    return filters;
  }

  /**
   * Add variant Info field into BQ table row.
   * If the info field number in the VCF header is `A`, add it to the ALT sub field.
   */
  public static void addInfo(TableRow row, VariantContext variantContext,
                             List<TableRow> altMetadata, VCFHeader vcfHeader) {
    for (Map.Entry<String, Object> entry : variantContext.getAttributes().entrySet()) {
      String attrName = entry.getKey();
      Object value = entry.getValue();
      VCFInfoHeaderLine infoMetadata = vcfHeader.getInfoHeaderLine(attrName);
      VCFHeaderLineType infoType = infoMetadata.getType();
      VCFHeaderLineCount infoCount = infoMetadata.getCountType();
      if (infoCount == VCFHeaderLineCount.A) {
        // Put this info into ALT field.
        splitAlternateAlleleInfoFields(attrName, value, altMetadata, infoType);
      } else {
        row.set(attrName, convertToDefinedType(value, infoType));
      }
    }
  }

  public static List<TableRow> addCalls(VariantContext variantContext, VCFHeader vcfHeader) {
    GenotypesContext genotypes = variantContext.getGenotypes();
    List<TableRow> callRows = new ArrayList<>();
    for (Genotype genotype : genotypes) {
      TableRow curRow = new TableRow();
      curRow.set(Constants.ColumnKeyConstants.CALLS_NAME, genotype.getSampleName());
      addInfoAndPhaseSet(curRow, genotype, vcfHeader);
      addGenotypes(curRow, genotype.getAlleles(), variantContext);
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
      // Deal with single value.
      // There are some field values that contains ',' but have not been parsed by HTSJDK,
      // eg: "HQ" ->"23,27", we need to convert it to list of String and call the convert function.
      if ((value instanceof String) && ((String)value).contains(",")) {
        String valueStr = (String)value;
        return convertToDefinedType(Arrays.asList(valueStr.split(",")), type);
      }
      return convertSingleObjectToDefinedType(value, type);
    } else {
      // Deal with list of values.
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
    // Values and their types have already been parsed by VCFCodec.decode() method.
    if (!(value instanceof String)) {
      return value;
    }

    String valueStr = (String)value;
    valueStr = valueStr.trim();   // For cases like " 27", ignore the space in list element.
    if (valueStr.equals(Constants.MISSING_FIELD_VALUE)) {
      return null;
    }

    // Double check if the String value match the type and also is a number.
    boolean isNumber = valueStr.matches("[-+]?\\d+(\\.\\d+)?");

    if (type == VCFHeaderLineType.Integer && isNumber) {
      return Integer.parseInt(valueStr);
    } else if (type == VCFHeaderLineType.Float && isNumber) {
      return Double.parseDouble(valueStr);
    } else {
      // Default String value.
      return value;
    }
  }

  private static void addGenotypes(TableRow row, List<Allele> alleles,
                                   VariantContext variantContext) {
    List<Integer> genotypes = new ArrayList<>();
    for (Allele allele : alleles) {
      String alleleStr = allele.getDisplayString();
      if (alleleStr.equals(Constants.MISSING_FIELD_VALUE)) {
        genotypes.add(Constants.MISSING_GENOTYPE_VALUE);
      } else {
        genotypes.add(variantContext.getAlleleIndex(allele));
      }
    }
    row.set(Constants.ColumnKeyConstants.CALLS_GENOTYPE, genotypes);
  }

  private static void addInfoAndPhaseSet(TableRow row, Genotype genotype, VCFHeader vcfHeader) {
    String phaseSet = "";

    if (genotype.hasAD()) { row.set("AD", genotype.getAD()); }
    if (genotype.hasDP()) { row.set("DP", genotype.getDP()); }
    if (genotype.hasGQ()) { row.set("GQ", genotype.getGQ()); }
    if (genotype.hasPL()) { row.set("PL", genotype.getPL()); }
    Map<String, Object> extendedAttributes = genotype.getExtendedAttributes();
    for (String extendedAttr : extendedAttributes.keySet()) {
      if (extendedAttr.equals(Constants.PHASESET_FORMAT_KEY)) {
        String phaseSetValue = extendedAttributes.get(Constants.PHASESET_FORMAT_KEY).toString();
        phaseSet = phaseSetValue.equals(Constants.MISSING_FIELD_VALUE) ? null : phaseSetValue;
      } else {
        // The rest of fields need to be converted to the right type by VCFCodec decode function.
        row.set(extendedAttr, convertToDefinedType(extendedAttributes.get(extendedAttr),
                vcfHeader.getFormatHeaderLine(extendedAttr).getType()));
      }
    }
    if (phaseSet == null || phaseSet.length() != 0) {
      // PhaseSet is presented(MISSING_FIELD_VALUE('.') or real value).
      row.set(Constants.ColumnKeyConstants.CALLS_PHASESET, phaseSet);
    } else {
      row.set(Constants.ColumnKeyConstants.CALLS_PHASESET, Constants.DEFAULT_PHASESET);
    }
  }

  private static void splitAlternateAlleleInfoFields(String attrName, Object value,
                                                     List<TableRow> altMetadata,
                                                     VCFHeaderLineType type) {
    if (value instanceof List) {
      List<Object> valList = (List<Object>)value;
      // If the alt field element size > 1, thus the value should be list and the size should be
      // equal to the alt field size.
      // But if alt field is ".", the altMetadata has only one TableRow with {"alt": null},
      // for every value in value list, they should add more {"alt": null} subrow if needed.
      // eg: altMetadata: {"alt": null}, alt info: AF=0.333,0.667
      // The result tableRow should be {"alt": null, AF=0.333}, {"alt":null, AF=0.667}.
      for (int i = 0; i < valList.size(); ++i) {
        if (i >= altMetadata.size()) {
          // should add more sub rows in alt field
          altMetadata.add(new TableRow());
          altMetadata.get(i).set(Constants.ColumnKeyConstants.ALTERNATE_BASES_ALT, null);
        }
        // Set attr into alt field.
        altMetadata.get(i).set(attrName, convertSingleObjectToDefinedType(valList.get(i), type));
      }
    } else {
      // The alt field element size == 1, thus the value should be a single list.
      altMetadata.get(0).set(attrName, convertSingleObjectToDefinedType(value, type));
    }
  }
}
