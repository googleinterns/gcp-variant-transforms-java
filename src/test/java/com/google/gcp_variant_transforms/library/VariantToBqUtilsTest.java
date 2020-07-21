// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.TableRow;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Units tests for VariantToBqUtils.java
 */
public class VariantToBqUtilsTest {
  private static final String ALTERNATE_BASES_ALT = "alt";
  private static final String CALLS_NAME = "name";
  private static final String CALLS_GENOTYPE = "genotype";
  private static final String CALLS_PHASESET = "phaseset";
  private static final String DEFAULT_PHASESET = "*";
  private static final String MISSING_FIELD_VALUE = ".";

  VariantContext variantContext;
  GenotypesContext genotypesContext;
  VCFHeader vcfHeader;

  @Before
  public void constructMockClasses() {
    variantContext = mock(VariantContext.class);
    vcfHeader = mock(VCFHeader.class);
    genotypesContext = mock(GenotypesContext.class);
  }

  @Test
  public void testConvertStringValueToRightValueType_whenComparingElement_thenTrue() {
    // test list of values
    String integerListStr = "23,27";
    assertThat(VariantToBqUtils.convertToDefinedType(integerListStr, VCFHeaderLineType.Integer))
            .isEqualTo(Arrays.asList(23, 27));
    String floatListStr = "0.333,0.667";
    assertThat(VariantToBqUtils.convertToDefinedType(floatListStr, VCFHeaderLineType.Float))
            .isEqualTo(Arrays.asList(0.333, 0.667));

    // test int values
    String integerStr = "23";
    assertThat(VariantToBqUtils.convertToDefinedType(integerStr, VCFHeaderLineType.Integer))
            .isEqualTo(23);

    // test float values
    String floatStr = "0.333";
    assertThat(VariantToBqUtils.convertToDefinedType(floatStr, VCFHeaderLineType.Float))
            .isEqualTo(0.333);

    // test String values
    String str = "T";
    assertThat(VariantToBqUtils.convertToDefinedType(str, VCFHeaderLineType.String))
            .isEqualTo("T");
  }

  @Test
  public void testBuildReferenceBase_whenComparingElement_thenTrue() {
    Allele refAllele = mock(Allele.class);
    when(refAllele.getDisplayString()).thenReturn("A");   // mock reference value
    when(variantContext.getReference()).thenReturn(refAllele);

    assertThat(VariantToBqUtils.buildReferenceBase(variantContext)).isEqualTo("A");
  }

  @Test
  public void testBuildNames_whenComparingElement_thenTrue() {
    when(variantContext.getID()).thenReturn("id");
    assertThat(VariantToBqUtils.buildNames(variantContext)).isEqualTo("id");
    // test empty fields
    when(variantContext.getID()).thenReturn(MISSING_FIELD_VALUE);
    assertThat(VariantToBqUtils.buildNames(variantContext)).isNull();

  }

  @Test
  public void testAddAlternates_whenComparingElement_thenTrue() {
    Allele altAllele = mock(Allele.class);
    when(altAllele.getDisplayString()).thenReturn("T");   // mock alternate value
    List<Allele> alternateList = new ArrayList<>();
    // test alt field is MISSING_FIELD_VALUE("."),
    // alternateList should be empty and TableRow should set null
    Map<String, Integer> alleleIndexingMap = new HashMap<>();
    when(variantContext.getAlternateAlleles()).thenReturn(alternateList);
    List<TableRow> missingFieldTableRow = VariantToBqUtils.addAlternates(variantContext,
            alleleIndexingMap);
    assertThat(missingFieldTableRow.get(0).get(ALTERNATE_BASES_ALT)).isNull();

    // test alt field has value "T"
    alternateList.add(altAllele);
    List<TableRow> altFieldTableRow = VariantToBqUtils.addAlternates(variantContext,
            alleleIndexingMap);
    assertThat(altFieldTableRow.get(0).get(ALTERNATE_BASES_ALT)).isEqualTo("T");
  }


  /**
   * The unit test for addInfo() method should cover the test of the private method
   * `splitAlternateAlleleInfoFields`, which put info fields with number = `A` into alt field
   * Test info record:
   * NS = 2, the NS field number = 1, type is Integer
   * AF = 0.333, the AF field number = A, type is Float
   */
  @Test
  public void testAddInfo_whenCheckingAltAndInfoFieldElements_thenTrue() {
    Map<String, Object> info = new HashMap<>();
    info.put("NS", "2");
    info.put("AF", "0.333");
    when(variantContext.getAttributes()).thenReturn(info);

    VCFInfoHeaderLine NSMetadata = mock(VCFInfoHeaderLine.class);
    when(vcfHeader.getInfoHeaderLine("NS")).thenReturn(NSMetadata);
    when(NSMetadata.getType()).thenReturn(VCFHeaderLineType.Integer);
    when(NSMetadata.getCountType()).thenReturn(VCFHeaderLineCount.INTEGER);

    VCFInfoHeaderLine AFMetadata = mock(VCFInfoHeaderLine.class);
    when(vcfHeader.getInfoHeaderLine("AF")).thenReturn(AFMetadata);
    when(AFMetadata.getType()).thenReturn(VCFHeaderLineType.Float);
    when(AFMetadata.getCountType()).thenReturn(VCFHeaderLineCount.A);

    // mock alt field
    List<TableRow> altMetadata = new ArrayList<>();
    altMetadata.add(new TableRow());
    altMetadata.get(0).set(ALTERNATE_BASES_ALT, "A");

    TableRow row = new TableRow();
    VariantToBqUtils.addInfo(row, variantContext, altMetadata, vcfHeader);

    assertThat(row.containsKey("NS")).isTrue();
    assertThat(row.get("NS")).isEqualTo(2);

    // AF field should not be in the info row, should be moved to alt field
    assertThat(!row.containsKey("AF")).isTrue();
    assertThat(altMetadata.get(0).containsKey("AF")).isTrue();
    assertThat(altMetadata.get(0).get("AF")).isEqualTo(0.333);
  }

  /**
   * The test for addCalls() methods should cover the test of the private method
   * 'addInfoAndPhaseSet' and 'addGenotypes'.
   * If phaseSet(PS) is presented, it should be removed from info map and instead set as phase set
   * If genotypes in missing, genotypes should be default value -1
   * If the phaseSet(PS) is not presented, phase set should be default "*"
   * Test Genotype:
   * FORMAT           sample
   * GT:DP:PS:HQ    1|2:1:0:23,27
   * GT:DP:HQ         .:1:.,.
   * In the second record, genotypes will set [-1,-1] and phase set will set "*",
   * and HQ will be [null, null]
   */
  @Test
  public void testAddCalls_whenCheckingGenotypeElements_thenTrue() {
    Genotype sample = mock(Genotype.class);
    when(sample.getSampleName()).thenReturn("sample");
    // mock genotypes
    Map<String, Integer> alleleIndexingMap = new HashMap<>();
    Allele firstAllele = mock(Allele.class);
    when(firstAllele.getDisplayString()).thenReturn("G");
    alleleIndexingMap.put("G", 1);
    Allele secondAllele = mock(Allele.class);
    when(secondAllele.getDisplayString()).thenReturn("A");
    alleleIndexingMap.put("A", 2);
    when(sample.getAlleles()).thenReturn(Arrays.asList(firstAllele, secondAllele));

    // mock info
    when(sample.hasAD()).thenReturn(false);
    when(sample.hasDP()).thenReturn(true);
    when(sample.getDP()).thenReturn(1);
    when(sample.hasGQ()).thenReturn(false);
    when(sample.hasPL()).thenReturn(false);
    Map<String, Object> extendedAttributes = new HashMap<>();
    extendedAttributes.put("HQ", "23,27");
    extendedAttributes.put("PS", "0");
    when(sample.getExtendedAttributes()).thenReturn(extendedAttributes);

    // mock VCF header
    VCFFormatHeaderLine DPMetadata = mock(VCFFormatHeaderLine.class);
    when(vcfHeader.getFormatHeaderLine("DP")).thenReturn(DPMetadata);
    when(DPMetadata.getType()).thenReturn(VCFHeaderLineType.Integer);
    when(DPMetadata.getCountType()).thenReturn(VCFHeaderLineCount.INTEGER);

    VCFFormatHeaderLine HQMetadata = mock(VCFFormatHeaderLine.class);
    when(vcfHeader.getFormatHeaderLine("HQ")).thenReturn(HQMetadata);
    when(HQMetadata.getType()).thenReturn(VCFHeaderLineType.Integer);
    when(HQMetadata.getCountType()).thenReturn(VCFHeaderLineCount.INTEGER);

    // add sample to genotypeContext
    Iterator<Genotype> genotypeIterator = mock(Iterator.class);
    when(genotypeIterator.hasNext()).thenReturn(true, false);
    when(genotypeIterator.next()).thenReturn(sample);
    when(genotypesContext.iterator()).thenReturn(genotypeIterator);

    // test table row fields
    List<TableRow> calls = VariantToBqUtils.addCalls(genotypesContext, vcfHeader,
            alleleIndexingMap);
    TableRow row = calls.get(0);
    assertThat(row.get(CALLS_NAME)).isEqualTo("sample");
    assertThat(row.get(CALLS_GENOTYPE)).isEqualTo(Arrays.asList(1, 2));
    assertThat(row.get(CALLS_PHASESET)).isEqualTo("0");
    // PS should not be in the info map, should be set in phase set
    assertThat(!row.containsKey("PS")).isTrue();
    assertThat(row.get("HQ")).isEqualTo(Arrays.asList(23,27));

    // Second record: contains empty fields
    // mock unknown genotypes
    when(firstAllele.getDisplayString()).thenReturn(MISSING_FIELD_VALUE);
    when(secondAllele.getDisplayString()).thenReturn(MISSING_FIELD_VALUE);
    when(sample.getAlleles()).thenReturn(Arrays.asList(firstAllele, secondAllele));

    // mock empty info fields
    extendedAttributes.remove("PS");
    extendedAttributes.put("HQ",".,.");
    when(sample.getExtendedAttributes()).thenReturn(extendedAttributes);

    // add sample to genotypeContext
    when(genotypesContext.iterator().hasNext()).thenReturn(true, false);
    when(genotypesContext.iterator().next()).thenReturn(sample);

    // test table row fields
    List<TableRow> callsWithEmptyFields = VariantToBqUtils.addCalls(genotypesContext, vcfHeader,
            alleleIndexingMap);
    TableRow rowWithEmptyFields = callsWithEmptyFields.get(0);
    assertThat(rowWithEmptyFields.get(CALLS_NAME)).isEqualTo("sample");
    assertThat(rowWithEmptyFields.get(CALLS_GENOTYPE)).isEqualTo(Arrays.asList(-1, -1));
    assertThat(rowWithEmptyFields.get(CALLS_PHASESET)).isEqualTo(DEFAULT_PHASESET);
    assertThat(rowWithEmptyFields.get("HQ")).isEqualTo(Arrays.asList(null,null));
  }
}
