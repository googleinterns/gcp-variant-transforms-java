// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gcp_variant_transforms.common.Constants;
import com.google.gcp_variant_transforms.exceptions.CountNotMatchException;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implementation class for {@link VariantToBqUtils}. It provides functionalities to set default
 * values and convert values to the types defined by VCF Header.
 */
public class VariantToBqUtilsImpl implements VariantToBqUtils, Serializable {
  public String getReferenceBases(VariantContext variantContext) {
    Allele referenceAllele = variantContext.getReference();
    String referenceBase = referenceAllele.getDisplayString();
    return replaceMissingWithNull(referenceBase);
  }

  public List<String> getNames(VariantContext variantContext) {
    String names = variantContext.getID();
    String[] splittedNamesBySemiColon = names.split(";");
    List<String> nameList = new ArrayList<>();
    for (String name : splittedNamesBySemiColon) {
      nameList.add(replaceMissingWithNull(name));
    }
    return nameList;
  }

  public List<TableRow> getAlternateBases(VariantContext variantContext) {
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

  public Set<String> getFilters(VariantContext variantContext) {
    Set<String> filters = variantContext.getFiltersMaybeNull();
    if (filters != null && filters.isEmpty()) {
      // If the filter is an empty set, it refers to "PASS".
      filters = new HashSet<>();
      filters.add("PASS");
    }
    return filters;
  }

  public void addInfo(TableRow row, VariantContext variantContext,
                      List<TableRow> altMetadata, VCFHeader vcfHeader, int expectedAltCount) {
    // Iterate all Info field in VCFHeader, if current record does not have the field, skip it.
    for (VCFInfoHeaderLine infoHeaderLine : vcfHeader.getInfoHeaderLines()) {
      String attrName = infoHeaderLine.getID();
      if (variantContext.hasAttribute(attrName)) {
        Object value = variantContext.getAttribute(attrName);
        VCFInfoHeaderLine infoMetadata = vcfHeader.getInfoHeaderLine(attrName);
        VCFHeaderLineType infoType = infoMetadata.getType();
        VCFHeaderLineCount infoCountType = infoMetadata.getCountType();
        if (infoCountType == VCFHeaderLineCount.A || infoCountType == VCFHeaderLineCount.R) {
          // If alternate field is ".", alternate alleles will be empty, expected count will be 1.
          if (infoCountType == VCFHeaderLineCount.A) {
            // Put this info into ALT field.
            splitAlternateAlleleInfoFields(attrName, value, altMetadata, infoType, expectedAltCount);
          } else {
            // field count should count all alleles, which is expectedAltCount plus reference.
            row.set(attrName, convertToDefinedType(value, infoType, expectedAltCount + 1));
          }
        } else if (infoCountType == VCFHeaderLineCount.INTEGER){
          row.set(attrName, convertToDefinedType(value, infoType, infoMetadata.getCount()));
        } else {
          // infoCountType is 'G' or '.', which we pass default count and we do not check if the count matches the
          // expected count.
          row.set(attrName, convertToDefinedType(value, infoType, Constants.DEFAULT_FIELD_COUNT));
        }
      }
    }
  }

  public List<TableRow> addCalls(VariantContext variantContext, VCFHeader vcfHeader) {
    GenotypesContext genotypes = variantContext.getGenotypes();
    List<TableRow> callRows = new ArrayList<>();
    for (Genotype genotype : genotypes) {
      TableRow curRow = new TableRow();
      curRow.set(Constants.ColumnKeyConstants.CALLS_NAME, genotype.getSampleName());
      addFormatAndPhaseSet(curRow, genotype, vcfHeader);
      addGenotypes(curRow, genotype.getAlleles(), variantContext);
      callRows.add(curRow);
    }
    return callRows;
  }

  public Object convertToDefinedType(Object value, VCFHeaderLineType type, int count) {
    if (!(value instanceof List)) {
      // Deal with single value.
      if ((value instanceof String) && ((String)value).contains(",")) {
        // Split string value.
        String valueStr = (String)value;
        return convertToDefinedType(Arrays.asList(valueStr.split(",")), type, count);
      } else if (count > 1) {
        // value is a single value but count > 1, it should raise an exception
        throw new CountNotMatchException("Value \"" + value + "\" size does not match the count defined by " +
            "VCFHeader");
      } else {
        return convertSingleObjectToDefinedType(value, type);
      }
    } else {
      // Deal with list of values.
      List<Object> valueList = (List<Object>)value;
      if (count != Constants.DEFAULT_FIELD_COUNT && count != valueList.size()) {
        throw new CountNotMatchException("Value \"" + value + "\" size does not match the count defined by " +
            "VCFHeader");
      }
      List<Object> convertedList = new ArrayList<>();
      for (Object val : valueList) {
        convertedList.add(convertSingleObjectToDefinedType(val, type));
      }
      return convertedList;
    }
  }

  public Object convertSingleObjectToDefinedType(Object value, VCFHeaderLineType type) {
    // If value is not a String, the values and its type have already been parsed by VCFCodec
    // decode function.
    if (!(value instanceof String)) {
      return value;
    }

    String valueStr = (String)value;
    valueStr = valueStr.trim();   // For cases like " 27", ignore the space in list element.
    if (valueStr.equals(VCFConstants.MISSING_VALUE_v4)) {
      return null;
    }

    if (type == VCFHeaderLineType.Integer) {
      // If the value is not an integer, it will raise an exception.
      return Integer.parseInt(valueStr);
    } else if (type == VCFHeaderLineType.Float) {
      return Double.parseDouble(valueStr);
    } else {
      // type is String
      return valueStr;
    }
  }

  public void addGenotypes(TableRow row, List<Allele> alleles,
                           VariantContext variantContext) {
    List<Integer> genotypes = new ArrayList<>();
    for (Allele allele : alleles) {
      String alleleStr = allele.getDisplayString();
      if (alleleStr.equals(VCFConstants.MISSING_VALUE_v4)) {
        genotypes.add(Constants.MISSING_GENOTYPE_VALUE);
      } else {
        genotypes.add(variantContext.getAlleleIndex(allele));
      }
    }
    row.set(Constants.ColumnKeyConstants.CALLS_GENOTYPE, genotypes);
  }

  public void addFormatAndPhaseSet(TableRow row, Genotype genotype, VCFHeader vcfHeader) {
    String phaseSet = "";
    // Iterate all format fields in VCFHeader.
    for (VCFFormatHeaderLine formatHeaderLine : vcfHeader.getFormatHeaderLines()) {
      String fieldName = formatHeaderLine.getID();
      if (fieldName.equals(VCFConstants.GENOTYPE_KEY)) {
        continue; // We will set GT field in a separate "genotype" field in BQ row.
      }
      if (genotype.hasAnyAttribute(fieldName)) {
        Object value = genotype.getAnyAttribute(fieldName);
        if (fieldName.equals(VCFConstants.GENOTYPE_ALLELE_DEPTHS) || fieldName.equals(VCFConstants.DEPTH_KEY) ||
            fieldName.equals(VCFConstants.GENOTYPE_QUALITY_KEY) || fieldName.equals(VCFConstants.GENOTYPE_PL_KEY)) {
          // These four field values have been pre-processed in Genotype.
          row.set(fieldName, value);
        } else if (fieldName.equals(VCFConstants.PHASE_SET_KEY)) {
          String phaseSetValue = genotype.getAnyAttribute(VCFConstants.PHASE_SET_KEY).toString();
          phaseSet = replaceMissingWithNull(phaseSetValue);
        } else {
          // The rest of fields need to be converted to the right type as VCFHeader specifies.
          VCFFormatHeaderLine formatMetadata = vcfHeader.getFormatHeaderLine(fieldName);
          VCFHeaderLineType formatType = formatMetadata.getType();
          VCFHeaderLineCount formatCountType = formatMetadata.getCountType();
          if (formatCountType == VCFHeaderLineCount.INTEGER) {
            row.set(fieldName, convertToDefinedType(genotype.getAnyAttribute(fieldName),
                formatType, formatMetadata.getCount()));
          } else {
            // If field number in the VCFHeader is ".", should pass a default count and do not check if count is equal
            // to the size of value.
            row.set(fieldName, convertToDefinedType(genotype.getAnyAttribute(fieldName),
                formatType, Constants.DEFAULT_FIELD_COUNT));
          }
        }
      }
    }
    if (phaseSet == null || !phaseSet.isEmpty()) {
      // PhaseSet is presented(MISSING_FIELD_VALUE('.') or a specific value).
      row.set(Constants.ColumnKeyConstants.CALLS_PHASESET, phaseSet);
    } else {
      row.set(Constants.ColumnKeyConstants.CALLS_PHASESET, Constants.DEFAULT_PHASESET);
    }
  }

  public void splitAlternateAlleleInfoFields(String attrName, Object value,
                                             List<TableRow> altMetadata,
                                             VCFHeaderLineType type, int count) {
    if (value instanceof List) {
      List<Object> valList = (List<Object>)value;
      if (count != valList.size()) {
        throw new CountNotMatchException("Value \"" + value + "\" size does not match the count defined by " +
            "VCFHeader");
      }
      for (int i = 0; i < valList.size(); ++i) {
        if (i >= altMetadata.size()) {
          // Match each field in each repeated subRow.
          altMetadata.add(new TableRow());
          altMetadata.get(i).set(Constants.ColumnKeyConstants.ALTERNATE_BASES_ALT, null);
        }
        // Set attr into alt field.
        altMetadata.get(i).set(attrName, convertSingleObjectToDefinedType(valList.get(i), type));
      }
    } else {
      // The alt field element size == 1, thus the value should be a single list.
      altMetadata.get(0).set(attrName, convertToDefinedType(value, type, 1));
    }
  }

  public String replaceMissingWithNull(String value) {
    return value.equals(VCFConstants.MISSING_VALUE_v4) ? null : value;
  }
}
