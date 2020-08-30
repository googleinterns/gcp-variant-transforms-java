// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableListIterator;
import com.google.gcp_variant_transforms.common.Constants;
import com.google.gcp_variant_transforms.TestEnv;
import com.google.guiceberry.junit4.GuiceBerryRule;
import com.google.inject.Inject;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Scanner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.xml.validation.Schema;

/**
 * Units tests for SchemaGeneratorImpl.java
 */
public class SchemaGeneratorImplTest {

  public final String SAMPLE_VCF_HEADER_FILEPATH = "src/test/java/com/google" +
      "/gcp_variant_transforms/data/SampleHeaderv4.2.vcf";
  public VCFHeader mockedVcfHeader;
  public VCFHeader sampleVcfHeader;

  @Inject public SchemaGeneratorImpl schemaGen;
  @Rule public final GuiceBerryRule guiceBerry = new GuiceBerryRule(TestEnv.class);
  
  @Before
  public void constructMockVCFHeader(){
    mockedVcfHeader = mock(VCFHeader.class);
  }

  @Before
  public void constructSampleVCFHeader(){
    // Header taken from VCF v4.2 specifications
    Scanner fileReader;
    ImmutableList.Builder<String> headerLines = new ImmutableList.Builder<>();
    try {
      fileReader = new Scanner(new File(SAMPLE_VCF_HEADER_FILEPATH));
    } catch (FileNotFoundException e){
      System.out.println("File SampleHeaderv4.3.vcf cannot be found.");
      return;
    }
    while(fileReader.hasNextLine()){
      headerLines.add(fileReader.nextLine());
    }
    VCFCodec vcfCodec = new VCFCodec();
    vcfCodec.readActualHeader(new HeaderIterator(headerLines.build()));
    sampleVcfHeader = vcfCodec.getHeader();
  }

  @Test
  public void testGetFields_whenGetReferenceField_thenIsEqualTo() {
    // Column: reference field
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(mockedVcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.REFERENCE_NAME);
    TableFieldSchema referenceField = fieldIterator.next();

    assertThat(referenceField.getName())
        .isEqualTo(Constants.ColumnKeyNames.REFERENCE_NAME);
    assertThat(referenceField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.REFERENCE_NAME);
    assertThat(referenceField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.NULLABLE);
    assertThat(referenceField.getType()).isEqualTo(SchemaUtils.BQFieldType.STRING);
  }

  @Test
  public void testGetFields_whenGetStartPosField_thenIsEqualTo() {
    // Column: start position
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(mockedVcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.START_POSITION);
    TableFieldSchema startPosField = fieldIterator.next();

    assertThat(startPosField.getName())
        .isEqualTo(Constants.ColumnKeyNames.START_POSITION);
    assertThat(startPosField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.START_POSITION);
    assertThat(startPosField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.NULLABLE);
    assertThat(startPosField.getType()).isEqualTo(SchemaUtils.BQFieldType.INTEGER);
  }

  @Test
  public void testGetFields_whenGetEndPosField_thenIsEqualTo() {
    // Column: end position
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(mockedVcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.END_POSITION);
    TableFieldSchema endPosField = fieldIterator.next();

    assertThat(endPosField.getName())
        .isEqualTo(Constants.ColumnKeyNames.END_POSITION);
    assertThat(endPosField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.END_POSITION);
    assertThat(endPosField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.NULLABLE);
    assertThat(endPosField.getType()).isEqualTo(SchemaUtils.BQFieldType.INTEGER);
  }

  @Test
  public void testGetFields_whenGetReferenceBasesField_thenIsEqualTo() {
    // Column: reference bases
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(mockedVcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.REFERENCE_BASES);
    TableFieldSchema referenceBasesField = fieldIterator.next();

    assertThat(referenceBasesField.getName())
        .isEqualTo(Constants.ColumnKeyNames.REFERENCE_BASES);
    assertThat(referenceBasesField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.REFERENCE_BASES);
    assertThat(referenceBasesField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.NULLABLE);
    assertThat(referenceBasesField.getType()).isEqualTo(SchemaUtils.BQFieldType.STRING);
  }

  @Test
  public void testGetFields_whenGetAlternateBasesField_thenIsEqualTo() {
    // Column: reference bases
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(mockedVcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.ALTERNATE_BASES);
    TableFieldSchema alternateBasesField = fieldIterator.next();

    assertThat(alternateBasesField.getName())
        .isEqualTo(Constants.ColumnKeyNames.ALTERNATE_BASES);
    assertThat(alternateBasesField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.ALTERNATE_BASES);
    assertThat(alternateBasesField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.REPEATED);
    assertThat(alternateBasesField.getType()).isEqualTo(SchemaUtils.BQFieldType.RECORD);
  }

  @Test
  public void testGetFields_whenGetNamesField_thenIsEqualTo() {
    // Column: names
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(mockedVcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.NAMES);
    TableFieldSchema namesField = fieldIterator.next();

    assertThat(namesField.getName())
        .isEqualTo(Constants.ColumnKeyNames.NAMES);
    assertThat(namesField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.NAMES);
    assertThat(namesField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.REPEATED);
    assertThat(namesField.getType()).isEqualTo(SchemaUtils.BQFieldType.STRING);
  }

  @Test
  public void testGetFields_whenGetQualityField_thenIsEqualTo() {
    // Column: quality
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(mockedVcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.QUALITY);
    TableFieldSchema qualityField = fieldIterator.next();

    assertThat(qualityField.getName())
        .isEqualTo(Constants.ColumnKeyNames.QUALITY);
    assertThat(qualityField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.QUALITY);
    assertThat(qualityField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.NULLABLE);
    assertThat(qualityField.getType()).isEqualTo(SchemaUtils.BQFieldType.FLOAT);
  }

  @Test
  public void testGetFields_whenGetFilterField_thenIsEqualTo() {
    // Column: filter
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(mockedVcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.FILTER);
    TableFieldSchema filterField = fieldIterator.next();

    assertThat(filterField.getName())
        .isEqualTo(Constants.ColumnKeyNames.FILTER);
    assertThat(filterField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.FILTER);
    assertThat(filterField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.REPEATED);
    assertThat(filterField.getType()).isEqualTo(SchemaUtils.BQFieldType.STRING);
  }

  @Test
  public void testGetFields_whenGetCallsField_thenIsEqualTo() {
    // Column: calls record
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(mockedVcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.CALLS);
    TableFieldSchema callsField = fieldIterator.next();

    assertThat(callsField.getName())
        .isEqualTo(Constants.ColumnKeyNames.CALLS);
    assertThat(callsField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.CALLS);
    assertThat(callsField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.REPEATED);
    assertThat(callsField.getType()).isEqualTo(SchemaUtils.BQFieldType.RECORD);
  }

  @Test
  public void testGetFields_whenGetCallsConstantSubFields_thenIsEqualTo() {
    // Columns: calls record's constant subfields
    // Verifies that the call record's sub-Fields maintain data integrity
    // after they are added to the call record in a FieldList.
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(mockedVcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.CALLS);
    TableFieldSchema callsField = fieldIterator.next();
    List<TableFieldSchema> callSubFields = callsField.getFields();
    TableFieldSchema callsSampleNameField = callSubFields
        .get(SchemaUtils.FieldIndex.CALLS_SAMPLE_NAME);
    TableFieldSchema callsGenotypeField = callSubFields
        .get(SchemaUtils.FieldIndex.CALLS_GENOTYPE);
    TableFieldSchema callsPhasesetField = callSubFields
        .get(SchemaUtils.FieldIndex.CALLS_PHASESET);

    // Call Sample Name asserts
    assertThat(callsSampleNameField.getName())
        .isEqualTo(Constants.ColumnKeyNames.CALLS_SAMPLE_NAME);
    assertThat(callsSampleNameField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.CALLS_SAMPLE_NAME);
    assertThat(callsSampleNameField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.NULLABLE);
    assertThat(callsSampleNameField.getType()).isEqualTo(SchemaUtils.BQFieldType.STRING);

    // Call Genotype asserts
    assertThat(callsGenotypeField.getName())
        .isEqualTo(Constants.ColumnKeyNames.CALLS_GENOTYPE);
    assertThat(callsGenotypeField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.CALLS_GENOTYPE);
    assertThat(callsGenotypeField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.REPEATED);
    assertThat(callsGenotypeField.getType()).isEqualTo(SchemaUtils.BQFieldType.INTEGER);

    // Call Phaseset asserts
    assertThat(callsPhasesetField.getName())
        .isEqualTo(Constants.ColumnKeyNames.CALLS_PHASESET);
    assertThat(callsPhasesetField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.CALLS_PHASESET);
    assertThat(callsPhasesetField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.NULLABLE);
    assertThat(callsPhasesetField.getType()).isEqualTo(SchemaUtils.BQFieldType.STRING);
  }

  @Test
  public void testGetCallSubFields_whenGetCallsSampleNameField_thenIsEqualTo() {
    // Column: calls sample Name
    ImmutableList<TableFieldSchema> callFields = schemaGen.getCallSubFields(mockedVcfHeader);
    TableFieldSchema callsSampleNameField = callFields
        .get(SchemaUtils.FieldIndex.CALLS_SAMPLE_NAME);

    assertThat(callsSampleNameField.getName())
        .isEqualTo(Constants.ColumnKeyNames.CALLS_SAMPLE_NAME);
    assertThat(callsSampleNameField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.CALLS_SAMPLE_NAME);
    assertThat(callsSampleNameField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.NULLABLE);
    assertThat(callsSampleNameField.getType()).isEqualTo(SchemaUtils.BQFieldType.STRING);
  }

  @Test
  public void testGetCallSubFields_whenGetCallsGenotypeField_thenIsEqualTo() {
    // Column: calls genotype
    ImmutableList<TableFieldSchema> callFields = schemaGen.getCallSubFields(mockedVcfHeader);
    TableFieldSchema callsGenotypeField = callFields
        .get(SchemaUtils.FieldIndex.CALLS_GENOTYPE);
    
    assertThat(callsGenotypeField.getName())
        .isEqualTo(Constants.ColumnKeyNames.CALLS_GENOTYPE);
    assertThat(callsGenotypeField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.CALLS_GENOTYPE);
    assertThat(callsGenotypeField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.REPEATED);
    assertThat(callsGenotypeField.getType()).isEqualTo(SchemaUtils.BQFieldType.INTEGER);
  }

  @Test
  public void testGetCallSubFields_whenGetCallsPhasesetField_thenIsEqualTo() {
    // Column: calls phaseset
    ImmutableList<TableFieldSchema> callFields = schemaGen.getCallSubFields(mockedVcfHeader);
    TableFieldSchema callsPhasesetField = callFields
        .get(SchemaUtils.FieldIndex.CALLS_PHASESET);

    assertThat(callsPhasesetField.getName())
        .isEqualTo(Constants.ColumnKeyNames.CALLS_PHASESET);
    assertThat(callsPhasesetField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.CALLS_PHASESET);
    assertThat(callsPhasesetField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.NULLABLE);
    assertThat(callsPhasesetField.getType()).isEqualTo(SchemaUtils.BQFieldType.STRING);
  }

  @Test
  public void testCreateInfoField_whenFunctionCall_thenIsEqualTo() {
    // Column: info field
    String name = "DP"; // Info ID
    int count = 1; // count is the Number from info header lines
    VCFHeaderLineType infoType = VCFHeaderLineType.Integer;
    String description = "Total Depth";
    String mode = SchemaUtils.BQFieldMode.NULLABLE;
    VCFInfoHeaderLine sampleInfoHeaderLine = new VCFInfoHeaderLine(name, count,
        infoType, description);

    TableFieldSchema infoField = schemaGen.convertCompoundHeaderLineToField(sampleInfoHeaderLine);

    assertThat(infoField.getName()).isEqualTo(name);
    assertThat(infoField.getDescription()).isEqualTo(description);
    assertThat(infoField.getType()).isEqualTo(SchemaUtils.BQFieldType.INTEGER);
    assertThat(infoField.getMode()).isEqualTo(mode);
  }

  @Test
  public void testCreateRecordField_whenCallsRecord_thenIsEqualTo() {
    // Column: Calls record dynamic fields
    String fieldName = Constants.ColumnKeyNames.CALLS;

    // Ordered based on 3 constant Call sub-fields(indices 0, 1, 2) and sample vcf header contents
    int genotypeIndex = 3;
    int genotypeQualityIndex = 4;
    int readDepthIndex = 5;
    int haplotypeQualityIndex = 6;

    String genotypeName = "GT";
    String genotypeQualityName = "GQ";
    String readDepthName = "DP";
    String haplotypeQualityName = "HQ";

    String genotypeDescription = "Genotype";
    String genotypeQualityDescription = "Genotype Quality";
    String readDepthDescription = "Read Depth";
    String haplotypeQualityDescription = "Haplotype Quality";

    TableFieldSchema callsField = schemaGen.createRecordField(sampleVcfHeader, fieldName);
    List<TableFieldSchema> callsSubFields = callsField.getFields();

    TableFieldSchema genotype = callsSubFields.get(genotypeIndex);
    TableFieldSchema genotypeQuality = callsSubFields.get(genotypeQualityIndex);
    TableFieldSchema readDepth = callsSubFields.get(readDepthIndex);
    TableFieldSchema haplotypeQuality = callsSubFields.get(haplotypeQualityIndex);

    // ID="GT" -- Genotype
    assertThat(genotype.getName()).isEqualTo(genotypeName);
    assertThat(genotype.getDescription()).isEqualTo(genotypeDescription);
    assertThat(genotype.getMode()).isEqualTo(SchemaUtils.BQFieldMode.NULLABLE);
    assertThat(genotype.getType()).isEqualTo(SchemaUtils.BQFieldType.STRING);

    // ID="GQ" -- Genotype Quality
    assertThat(genotypeQuality.getName()).isEqualTo(genotypeQualityName);
    assertThat(genotypeQuality.getDescription()).isEqualTo(genotypeQualityDescription);
    assertThat(genotypeQuality.getMode()).isEqualTo(SchemaUtils.BQFieldMode.NULLABLE);
    assertThat(genotypeQuality.getType()).isEqualTo(SchemaUtils.BQFieldType.INTEGER);

    // ID="DP" -- Read Depth
    assertThat(readDepth.getName()).isEqualTo(readDepthName);
    assertThat(readDepth.getDescription()).isEqualTo(readDepthDescription);
    assertThat(readDepth.getMode()).isEqualTo(SchemaUtils.BQFieldMode.NULLABLE);
    assertThat(readDepth.getType()).isEqualTo(SchemaUtils.BQFieldType.INTEGER);

    // ID="HQ" -- Haplotype Quality
    assertThat(haplotypeQuality.getName()).isEqualTo(haplotypeQualityName);
    assertThat(haplotypeQuality.getDescription()).isEqualTo(haplotypeQualityDescription);
    assertThat(haplotypeQuality.getMode()).isEqualTo(SchemaUtils.BQFieldMode.NULLABLE);
    assertThat(haplotypeQuality.getType()).isEqualTo(SchemaUtils.BQFieldType.INTEGER);
  }

  @Test
  public void testCreateRecordField_whenAlternateBasesRecord_thenIsEqualTo() {
    // Alternate Bases record subfields including INFO headerlines with Number='A'
    String fieldName = Constants.ColumnKeyNames.ALTERNATE_BASES;

    // Ordered based on 1 constant alt base sub-field(index 0) and sample vcf header contents
    int altBaseIndex = 0;
    int alleleFrequencyIndex = 1;
    String alleleFrequencyName = "AF";
    String alleleFrequencyDescription = "Allele Frequency";

    TableFieldSchema altField = schemaGen.createRecordField(sampleVcfHeader, fieldName);
    List<TableFieldSchema> altSubFields = altField.getFields();
    TableFieldSchema alleleFrequency = altSubFields.get(alleleFrequencyIndex);
    TableFieldSchema altBaseField = altSubFields.get(altBaseIndex);

    // Alternate Bases Alt
    assertThat(altBaseField.getName()).isEqualTo(Constants.ColumnKeyNames.ALTERNATE_BASES_ALT);
    assertThat(altBaseField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.ALTERNATE_BASES_ALT);
    assertThat(altBaseField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.NULLABLE);
    assertThat(altBaseField.getType()).isEqualTo(SchemaUtils.BQFieldType.STRING);

    // ID="AF" -- Genotype
    assertThat(alleleFrequency.getName()).isEqualTo(alleleFrequencyName);
    assertThat(alleleFrequency.getDescription()).isEqualTo(alleleFrequencyDescription);
    assertThat(alleleFrequency.getMode()).isEqualTo(SchemaUtils.BQFieldMode.NULLABLE);
    assertThat(alleleFrequency.getType()).isEqualTo(SchemaUtils.BQFieldType.FLOAT);
  }

  @Test
  public void testGetSanitizedFieldName_whenInvalidStart_thenIsEqualTo() {
    String fieldName = "9Invalid_name";
    String expectedFieldName ="field_9Invalid_name";
    String actualFieldName = SchemaUtils.getSanitizedFieldName(fieldName);

    assertThat(actualFieldName).isEqualTo(expectedFieldName);
  }

  @Test
  public void testGetSanitizedFieldName_whenInvalidSymbol_thenIsEqualTo() {
    String fieldName = "Invalid_n@me";
    String expectedFieldName ="Invalid_n_me";
    String actualFieldName = SchemaUtils.getSanitizedFieldName(fieldName);

    assertThat(actualFieldName).isEqualTo(expectedFieldName);
  }

  @Test
  public void testGetSanitizedFieldName_whenValid_thenIsEqualTo() {
    String fieldName = "valid_name";
    String actualFieldName = SchemaUtils.getSanitizedFieldName(fieldName);

    assertThat(actualFieldName).isEqualTo(fieldName); // Should be unchanged.
  }
}
