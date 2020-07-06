// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.entity;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Internal representation of Variant Data.
 */
public class Variant implements Serializable {
  private static final long serialVersionUID = 260660913763642347L;
  private static final String PHASESET_FORMAT_KEY = "PS";   // The phaseset format key.
  private static final String MISSING_FIELD_VALUE = ".";
  private static final String DEFAULT_PHASESET_VALUE = "*";
  // Default phaseset value if call is phased, but no 'PS' is present.
  private static final int MISSING_GENOTYPE_VALUE = -1;

  private final String contig;
  private final String referenceBase;
  private final int start;
  private final int end;
  private final double quality;

  private List<String> alternateBases;
  private List<String> names;
  private Set<String> filters;
  private Map<String, Object> info;
  private List<VariantCall> calls;

  /**
   * @param contig    Reference on which this variant occurs (such as `chr20` or `X`)
   * @param start     The position at which this variant occurs, which is 1-based
   * @param end       The end position of this variant. Corresponds to the first base
   *                  after the last base in the reference allele.
   * @param referenceBase   The reference bases for this variant.
   * @param alternateBases  The bases that appear instead of the reference bases.
   * @param names     Names for the variant, for example a RefSNP ID.
   * @param quality   Phred-scaled quality score (-10log10 prob(call is wrong)).
   *                  Higher values imply better quality.
   * @param filters   A list of filters (normally quality filters) this variant has failed.
   *                 `PASS` indicates this variant has passed all filters.
   * @param info      A map of additional variant information. The key is specified in the
   *                  VCF record and the value can be any format.
   * @param calls     The variant calls for this variant. Each one represents the determination
   *                  of genotype with respect to this variant.
   */
  public Variant(String contig, int start, int end, String referenceBase,
                 List<String> alternateBases, List<String> names, double quality,
                 Set<String> filters, Map<String, Object> info, List<VariantCall> calls) {
    this.contig = contig;
    this.start = start;
    this.end = end;
    this.referenceBase = referenceBase;
    this.alternateBases = alternateBases;
    this.names = names;
    this.quality = quality;
    this.filters = filters;
    this.info = info;
    this.calls = calls;
  }

  public Variant(VariantContext variantContext, VCFHeader vcfHeader) {
    this.contig = variantContext.getContig();
    this.start = variantContext.getStart();
    this.end = variantContext.getEnd();
    this.referenceBase = buildReferenceBase(variantContext);
    this.alternateBases = buildAlternateBaseStrings(variantContext.getAlternateAlleles());
    this.names = buildNames(variantContext);
    this.quality = variantContext.getPhredScaledQual();
    this.filters = variantContext.getFilters();
    this.info = buildAttributes(variantContext, vcfHeader);
    this.calls = buildVariantCalls(variantContext, vcfHeader);
  }

  public String getContig() {
    return contig;
  }

  public int getStart() {
    return start;
  }

  public int getEnd() {
    return end;
  }

  public String getReferenceBase() {
    return referenceBase;
  }

  public double getQuality() {
    return quality;
  }

  public List<String> getAlternateBases() {
    return alternateBases;
  }

  public List<String> getNames() {
    return names;
  }

  public Set<String> getFilters() {
    return filters;
  }

  public Map<String, Object> getInfo() {
    return info;
  }

  public List<VariantCall> getCalls() {
    return calls;
  }

  // reference base will not have MISSING_VALUE(".")
  private String buildReferenceBase(VariantContext variantContext) {
    Allele referenceAllele = variantContext.getReference();
    return referenceAllele.getBaseString();
  }

  /**
   * Alternate bases in the ALT field, for example, T or .
   */
  private List<String> buildAlternateBaseStrings(List<Allele> alternateAllelesList) {
    List<String> alternateBases = new ArrayList<>();
    // if field value is '.', alternateAllelesList will be empty
    if (alternateAllelesList.isEmpty()) {
      alternateBases.add(null);
      return alternateBases;
    }

    for (Allele allele : alternateAllelesList) {
      String alleleStr = allele.getDisplayString();
      alternateBases.add(alleleStr.equals(MISSING_FIELD_VALUE) ? null : alleleStr);
    }
    return alternateBases;
  }

  /**
   * alternate bases(GT field) in the genotype, for example, 1|2 or .
   */
  private List<Integer> buildAlternateBases(List<Allele> alternateAllelesList) {
    List<Integer> alternateBases = new ArrayList<>();
    for (Allele allele : alternateAllelesList) {
      byte[] bases = allele.getDisplayBases();
      if (bases.length == 0) {
        // MISSING_VALUE is presented, should add -1
        alternateBases.add(MISSING_GENOTYPE_VALUE);
      } else {
        for (byte base : bases) {
          alternateBases.add((int)base);
        }
      }
    }
    return alternateBases;
  }

  /**
   * get ID field from VariantContext
   */
  private List<String> buildNames(VariantContext variantContext) {
    List<String> names = new ArrayList<>();
    // FIXME: varaintContext.getID() only returns single String, is there multi IDs in ID field?
    String name = variantContext.getID();
    names.add(name.equals(MISSING_FIELD_VALUE) ? null : name);
    return names;
  }

  /**
   * for INFO field, for example: NS=2;DP=10;AF=0.333,0.667;AA=T;DB
   * Convert the Variant attributes into the types that matches the header lines define
   */
  private Map<String, Object> buildAttributes(VariantContext context, VCFHeader header) {
    Map<String, Object> info = new HashMap<>();
    for (Map.Entry<String, Object> entry : context.getAttributes().entrySet()) {
      String attrName = entry.getKey();
      Object value = entry.getValue();
      VCFInfoHeaderLine infoMetadata = header.getInfoHeaderLine(attrName);

      if (infoMetadata == null) {
        // The metadata has not defined the type, use the default type in the VariantContext
        info.put(attrName, value);
        continue;
      }

      VCFHeaderLineType infoType = infoMetadata.getType();
      info.put(attrName, convertToDefinedType(value, infoType));
    }
    return info;
  }

  /**
   * Convert the value to the type defined in the metadata
   * If the value is an instance of List, iterate the list
   * and call convertSingleObjectToDefinedType() to convert every value
   */
  private Object convertToDefinedType(Object value, VCFHeaderLineType type) {
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
  private Object convertSingleObjectToDefinedType(Object value, VCFHeaderLineType type) {
    // values and their types have already been parsed by VCFCodec.decode()
    if (!(value instanceof String)) {
      return value;
    }

    String valueStr = (String)value;
    valueStr = valueStr.trim();   // for cases like "23, 27", ignore the space in list element

    if (valueStr.equals(MISSING_FIELD_VALUE)) {
      return null;
    }

    // FIXME: HTSJDK getAttributeAsInt() provides the default value as type int(not Integer), we
    //  cannot use the method to return a null when '.' is presented.
    // FIXME: If MISSING_VALUE('.') is returned null before, do we need to double check if the
    //    value is a number?

    // check if the String value match the type and also is a number
    // eg: value = "." but type is Integer, it should not call parseInt
    boolean isNumber = valueStr.matches("[-+]?\\d+(\\.\\d+)?");

    if (type == VCFHeaderLineType.Integer && isNumber) {
      return Integer.parseInt(valueStr);
    } else if (type == VCFHeaderLineType.Float && isNumber) {
      return Double.parseDouble(valueStr);
    } else {
      // default String value or boolean
      return value;
    }

  }


  /**
   * Convert the VariantContext Genotypes into a list of VariantCalls
   */
  private List<VariantCall> buildVariantCalls(VariantContext context, VCFHeader header) {
    List<VariantCall> variantCalls = new ArrayList<>();
    GenotypesContext genotypesContext = context.getGenotypes();
    for (Genotype genotype : genotypesContext) {
      String sampleName = genotype.getSampleName();
      List<Integer> genotypes = buildAlternateBases(genotype.getAlleles());
      // phase set value will update if "PS" is presented
      Map<String, Object> genotypeInfo = new HashMap<>();
      String phaseSet = constructGenotypeInfo(genotype, genotypeInfo, header);
      VariantCall call = new VariantCall(sampleName, genotypes, phaseSet, genotypeInfo);
      variantCalls.add(call);
    }
    return variantCalls;
  }

  /**
   * construct the Info Map, and return the phaseSet.
   * if "PS" is presented, skip adding to info map and set the Phase Set, otherwise return "*"
   */
  private String constructGenotypeInfo(Genotype genotype, Map<String,
          Object> genotypeInfo, VCFHeader header) {
    // The first 4 field have been converted to the right type by VCFCodec decode function
    // FIXME: Will AD be '.'? getAD() will return int[], cannot check if it is '.'(need more test)
    if (genotype.hasAD()) { genotypeInfo.put("AD", genotype.getAD()); }
    if (genotype.hasDP()) { genotypeInfo.put("DP", genotype.getDP()); }
    if (genotype.hasGQ()) { genotypeInfo.put("GQ", genotype.getGQ()); }
    if (genotype.hasPL()) { genotypeInfo.put("PL", genotype.getPL()); }
    String phaseSet = "";
    Map<String, Object> extendedAttributes = genotype.getExtendedAttributes();
    for (String extendedAttr : extendedAttributes.keySet()) {
      if (extendedAttr.equals(PHASESET_FORMAT_KEY)) {
        // FIXME: In python code, missing phaseSet will set as "*", how about PS = '.'?
        //    Should be * or null? For now, it is set as "*"
        String phaseSetValue = extendedAttributes.get(PHASESET_FORMAT_KEY).toString();
        if (phaseSetValue.equals(MISSING_FIELD_VALUE)) { continue; }
        phaseSet = phaseSetValue;
      } else {
        // The rest of fields need to be converted to the right type by VCFCodec decode function
        genotypeInfo.put(extendedAttr, convertToDefinedType(extendedAttributes.get(extendedAttr),
                header.getFormatHeaderLine(extendedAttr).getType()));
      }
    }
    // if phase set has not been set by valid PS field value, it will set as "*"
    return phaseSet.length() == 0 ? "*" : phaseSet;
  }
}