// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.api.services.bigquery.model.TableRow;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;
import java.util.List;
import java.util.Set;

/**
 * Service to convert field in {@link VariantContext} to BQ table required format.
 */
public interface VariantToBqUtils {
  /**
   * Get reference allele bases. Set null in this field if there is missing value.
   * @param variantContext
   * @return reference bases after handling missing value.
   */
  public String getReferenceBases(VariantContext variantContext);

  /**
   * Get names from VariantContext's ID field. Set null in this field if there is missing value.
   * @param variantContext
   * @return names after handling missing value.
   */
  public String getNames(VariantContext variantContext);

  /**
   * Get alternate allele bases. Set null in this field if there is missing value.
   * @param variantContext
   * @return List of {@link TableRow} setting each alternate base in the alternate base field.
   */
  public List<TableRow> getAlternateBases(VariantContext variantContext);

  /**
   * Get filter set. If it is an empty set, it is required to add "PASS" in the filter.
   * @param variantContext
   * @return Filter Set after handling "PASS" and missing value.
   */
  public Set<String> getFilters(VariantContext variantContext);

  /**
   * Add variant Info field into BQ table row. For each field value, check if the value size
   * matches the count defined by VCFHeader and convert each value to the defined type.
   * If the info field number in the VCF header is `A`, add it to the ALT sub field.
   * @param row Base TableRow for the VariantContext.
   * @param variantContext
   * @param altMetadata List of TableRow in the alternate bases repeated field.
   * @param vcfHeader
   */
  public void addInfo(TableRow row, VariantContext variantContext, List<TableRow> altMetadata,
                      VCFHeader vcfHeader);

  /**
   * Add calls(samples) in the table row with {@link VCFHeader} defined value type and count.
   * @param variantContext
   * @param vcfHeader Provides value type and count format.
   * @return List of TableRow in the calls repeated field in the TableRow.
   */
  public List<TableRow> addCalls(VariantContext variantContext, VCFHeader vcfHeader);

  /**
   * <p>
   * Convert object value to the {@link VCFHeader} defined type and count.
   * If the value size does not match the count in the VCFHeader, it should raise an exception.
   * For list of values, for each value,call `convertSingleObjectToDefinedType` to convert value
   * to defined type.
   * </p>
   *
   * <p>
   *  Notice that there are some field values that contains ',' but have not been parsed by HTSJDK,
   *  eg: "HQ" ->"23,27", we need to split it to list of String and call the convert function.
   * </p>
   * @param value Raw value in VariantContext.
   * @param type Field value type defined by VCFHeader.
   * @param count Field count defined by VCFHeader.
   * @return Object value with formatted type and count.
   */
  public Object convertToDefinedType(Object value, VCFHeaderLineType type, int count);

  /**
   * Convert single value to {@link VCFHeader} defined type.
   * @param value Single raw value in VariantContext.
   * @param type Field value type defined by VCFHeader.
   * @return Object value with formatted type.
   */
  public Object convertSingleObjectToDefinedType(Object value, VCFHeaderLineType type);

  /**
   * Get the genotype in {@link Genotype}, the value is in index to the genotype allele in the
   * allele list. If there is missing value("."), add default value -1.
   * @param row TableRow for each call(sample).
   * @param alleles allele list for the VCF record.
   * @param variantContext
   */
  public void addGenotypes(TableRow row, List<Allele> alleles, VariantContext variantContext);

  /**
   * Add info field in {@link Genotype}, if there is phase set field(PS), set phase set. If there
   * is no such phase set field, set default phase set "*".
   * @param row TableRow for each call(sample).
   * @param genotype Current genotype sample in the VariantContext
   * @param vcfHeader Define the field type and count format.
   */
  public void addInfoAndPhaseSet(TableRow row, Genotype genotype, VCFHeader vcfHeader);

  /**
   * <p>
   *  Move fields in INFO field that number = `A` defined in the VCFHeader to alternate_bases
   *  nested field, since those field count is related to the alternate bases. The alternate bases
   *  field could have repeated subfields with alternate names and fields with number = 'A'.
   * </p>
   *
   * <p>
   *  Notice that if the alt field element size > 1, thus the value should be list and the size
   *  should be equal to the alt field size. But if alt field is ".", the altMetadata has only
   *  one TableRow with {"alt": null}, for every value in value list, they should add more {"alt
   *  ": null} subrow if needed.
   *  eg: altMetadata: {"alt": null}, alt info: AF=0.333,0.667
   *  The result tableRow should be {"alt": null, AF=0.333}, {"alt":null, AF=0.667}.
   * </p>
   * @param attrName
   * @param value
   * @param altMetadata
   * @param type
   * @param count
   */
  public void splitAlternateAlleleInfoFields(String attrName, Object value,
                                             List<TableRow> altMetadata, VCFHeaderLineType type,
                                             int count);
}
