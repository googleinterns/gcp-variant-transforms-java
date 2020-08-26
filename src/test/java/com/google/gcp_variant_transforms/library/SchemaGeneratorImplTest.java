// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableListIterator;
import com.google.gcp_variant_transforms.TestEnv;
import com.google.gcp_variant_transforms.common.Constants;
import com.google.guiceberry.junit4.GuiceBerryRule;
import com.google.inject.Inject;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Units tests for SchemaGeneratorImpl.java
 */
public class SchemaGeneratorImplTest {

  public VCFHeader vcfHeader;
  
  @Inject public SchemaGeneratorImpl schemaGen;
  @Rule public final GuiceBerryRule guiceBerry = new GuiceBerryRule(TestEnv.class);
  
  @Before
  public void constructMockVCFHeader(){
    vcfHeader = mock(VCFHeader.class);
  }

  @Test
  public void testGetFields_whenGetReferenceField_thenIsEqualTo() {
    // Column: reference field
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
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
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
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
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
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
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
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
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
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
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
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
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
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
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
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
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
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
  public void testGetFields_whenGetCallsSubFields_thenIsEqualTo() {
    // Columns: calls record's subfields
    // Verifies that the call record's sub-Fields maintain data integrity
    // after they are added to the call record in a FieldList.
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
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
    ImmutableList<TableFieldSchema> callFields = schemaGen.getCallSubFields(vcfHeader);
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
    ImmutableList<TableFieldSchema> callFields = schemaGen.getCallSubFields(vcfHeader);
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
    ImmutableList<TableFieldSchema> callFields = schemaGen.getCallSubFields(vcfHeader);
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

    TableFieldSchema infoField = schemaGen.createInfoField(sampleInfoHeaderLine);

    assertThat(infoField.getName()).isEqualTo(name);
    assertThat(infoField.getDescription()).isEqualTo(description);
    assertThat(infoField.getType()).isEqualTo(SchemaUtils.BQFieldType.INTEGER);
    assertThat(infoField.getMode()).isEqualTo(mode);
  }

  @Test
  public void testCreateRecordField_whenCallsRecord_thenIsEqualTo() {
    // Column: Calls record
    String fieldName = Constants.ColumnKeyNames.CALLS;

    TableFieldSchema infoField = schemaGen.createRecordField(vcfHeader, fieldName);

    assertThat(infoField.getName()).isEqualTo(Constants.ColumnKeyNames.CALLS);
    assertThat(infoField.getDescription()).isEqualTo(SchemaUtils.FieldDescription.CALLS);
    assertThat(infoField.getType()).isEqualTo(SchemaUtils.BQFieldType.RECORD);
    assertThat(infoField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.REPEATED);
  }

  @Test
  public void testCreateRecordField_whenAlternateBasesRecord_thenIsEqualTo() {
    // Column: Alternate Bases record
    String fieldName = Constants.ColumnKeyNames.ALTERNATE_BASES;

    TableFieldSchema infoField = schemaGen.createRecordField(vcfHeader, fieldName);

    assertThat(infoField.getName()).isEqualTo(Constants.ColumnKeyNames.ALTERNATE_BASES);
    assertThat(infoField.getDescription()).isEqualTo(SchemaUtils.FieldDescription.ALTERNATE_BASES);
    assertThat(infoField.getType()).isEqualTo(SchemaUtils.BQFieldType.RECORD);
    assertThat(infoField.getMode()).isEqualTo(SchemaUtils.BQFieldMode.REPEATED);
  }
}
