// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableListIterator;
import com.google.guiceberry.junit4.GuiceBerryRule;
import com.google.gcp_variant_transforms.TestEnv;
import com.google.inject.Inject;
import htsjdk.variant.vcf.VCFHeader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Units tests for SchemaGeneratorImpl.java
 */
public class SchemaGeneratorImplTest {
  private final int REFERENCE_INDEX = 0;
  private final int START_POS_INDEX = 1;
  private final int END_POS_INDEX = 2;
  private final int REFERENCE_BASES_INDEX = 3;
  private final int NAMES_INDEX = 4;
  private final int QUALITY_INDEX = 5;
  private final int FILTER_INDEX = 6;
  private final int CALLS_INDEX = 7;
  private final int CALLS_SAMPLE_ID_INDEX = 0;
  private final int CALLS_GENOTYPE_INDEX = 1;
  private final int CALLS_PHASESET_INDEX = 2;

  public VCFHeader vcfHeader;
  
  @Inject public SchemaGeneratorImpl schemaGen;
  @Rule public final GuiceBerryRule guiceBerry = new GuiceBerryRule(TestEnv.class);
  
  @Before
  public void constructMockVCFHeader(){
    vcfHeader = mock(VCFHeader.class);
  }

  @Test
  //column: reference field
  public void testGetFields_whenGetReferenceField_thenIsEqualTo() {
    ImmutableList<Field> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<Field> fieldIterator = fields.listIterator(REFERENCE_INDEX);
    Field referenceField = fieldIterator.next();

    assertThat(referenceField.getName())
        .isEqualTo(VariantToBqUtils.ColumnKeyConstants.REFERENCE_NAME);
    assertThat(referenceField.getDescription())
        .isEqualTo(SchemaGeneratorImpl.FieldDescriptionConstants.REFERENCE_NAME);
    assertThat(referenceField.getMode()).isEqualTo(Field.Mode.NULLABLE);
    //getType returns a LegacySQLTypeName even if the Field's type is set with StandardSQLTypeName 
    assertThat(referenceField.getType()).isEqualTo(LegacySQLTypeName.STRING);
  }

  @Test
  //column: start position
  public void testGetFields_whenGetStartPosField_thenIsEqualTo() {
    ImmutableList<Field> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<Field> fieldIterator = fields.listIterator(START_POS_INDEX);
    Field startPosField = fieldIterator.next();

    assertThat(startPosField.getName())
        .isEqualTo(VariantToBqUtils.ColumnKeyConstants.START_POSITION);
    assertThat(startPosField.getDescription())
        .isEqualTo(SchemaGeneratorImpl.FieldDescriptionConstants.START_POSITION);
    assertThat(startPosField.getMode()).isEqualTo(Field.Mode.NULLABLE);
    assertThat(startPosField.getType()).isEqualTo(LegacySQLTypeName.INTEGER);
  }

  @Test
  //column: end position
  public void testGetFields_whenGetEndPosField_thenIsEqualTo() {
    ImmutableList<Field> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<Field> fieldIterator = fields.listIterator(END_POS_INDEX);
    Field endPosField = fieldIterator.next();

    assertThat(endPosField.getName())
        .isEqualTo(VariantToBqUtils.ColumnKeyConstants.END_POSITION);
    assertThat(endPosField.getDescription())
        .isEqualTo(SchemaGeneratorImpl.FieldDescriptionConstants.END_POSITION);
    assertThat(endPosField.getMode()).isEqualTo(Field.Mode.NULLABLE);
    assertThat(endPosField.getType()).isEqualTo(LegacySQLTypeName.INTEGER);
  }

  @Test
  //column: reference bases
  public void testGetFields_whenGetReferenceBasesField_thenIsEqualTo() {
    ImmutableList<Field> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<Field> fieldIterator = fields.listIterator(REFERENCE_BASES_INDEX);
    Field referenceBasesField = fieldIterator.next();

    assertThat(referenceBasesField.getName())
        .isEqualTo(VariantToBqUtils.ColumnKeyConstants.REFERENCE_BASES);
    assertThat(referenceBasesField.getDescription())
        .isEqualTo(SchemaGeneratorImpl.FieldDescriptionConstants.REFERENCE_BASES);
    assertThat(referenceBasesField.getMode()).isEqualTo(Field.Mode.NULLABLE);
    assertThat(referenceBasesField.getType()).isEqualTo(LegacySQLTypeName.STRING);
  }

  @Test
  //column: names
  public void testGetFields_whenGetNamesField_thenIsEqualTo() {
    ImmutableList<Field> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<Field> fieldIterator = fields.listIterator(NAMES_INDEX);
    Field namesField = fieldIterator.next();

    assertThat(namesField.getName())
        .isEqualTo(VariantToBqUtils.ColumnKeyConstants.NAMES);
    assertThat(namesField.getDescription())
        .isEqualTo(SchemaGeneratorImpl.FieldDescriptionConstants.NAMES);
    assertThat(namesField.getMode()).isEqualTo(Field.Mode.REPEATED);
    assertThat(namesField.getType()).isEqualTo(LegacySQLTypeName.STRING);
  }

  @Test
  //column: quality
  public void testGetFields_whenGetQualityField_thenIsEqualTo() {
    ImmutableList<Field> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<Field> fieldIterator = fields.listIterator(QUALITY_INDEX);
    Field qualityField = fieldIterator.next();

    assertThat(qualityField.getName())
        .isEqualTo(VariantToBqUtils.ColumnKeyConstants.QUALITY);
    assertThat(qualityField.getDescription())
        .isEqualTo(SchemaGeneratorImpl.FieldDescriptionConstants.QUALITY);
    assertThat(qualityField.getMode()).isEqualTo(Field.Mode.NULLABLE);
    assertThat(qualityField.getType()).isEqualTo(LegacySQLTypeName.FLOAT);
  }

  @Test
  //column: filter
  public void testGetFields_whenGetFilterField_thenIsEqualTo() {
    ImmutableList<Field> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<Field> fieldIterator = fields.listIterator(FILTER_INDEX);
    Field filterField = fieldIterator.next();

    assertThat(filterField.getName())
        .isEqualTo(VariantToBqUtils.ColumnKeyConstants.FILTER);
    assertThat(filterField.getDescription())
        .isEqualTo(SchemaGeneratorImpl.FieldDescriptionConstants.FILTER);
    assertThat(filterField.getMode()).isEqualTo(Field.Mode.REPEATED);
    assertThat(filterField.getType()).isEqualTo(LegacySQLTypeName.STRING);
  }

  @Test
  //column: calls record
  public void testGetFields_whenGetCallsField_thenIsEqualTo() {
    ImmutableList<Field> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<Field> fieldIterator = fields.listIterator(CALLS_INDEX);
    Field callsField = fieldIterator.next();

    assertThat(callsField.getName())
        .isEqualTo(VariantToBqUtils.ColumnKeyConstants.CALLS);
    assertThat(callsField.getDescription())
        .isEqualTo(SchemaGeneratorImpl.FieldDescriptionConstants.CALLS);
    assertThat(callsField.getMode()).isEqualTo(Field.Mode.REPEATED);
    assertThat(callsField.getType()).isEqualTo(LegacySQLTypeName.RECORD);
  }

  @Test
  //column: calls record's subfields
  // verifies that the call record's sub-Fields maintain data integrity 
  // after they are added to the call record in a FieldList 
  public void testGetFields_whenGetCallsSubFields_thenIsEqualTo() {
    ImmutableList<Field> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<Field> fieldIterator = fields.listIterator(CALLS_INDEX);
    Field callsField = fieldIterator.next();
    FieldList callSubFields = callsField.getSubFields();
    Field callsSampleIDField = callSubFields.get(CALLS_SAMPLE_ID_INDEX);
    Field callsGenotypeField = callSubFields.get(CALLS_GENOTYPE_INDEX);
    Field callsPhasesetField = callSubFields.get(CALLS_PHASESET_INDEX);


    assertThat(callsSampleIDField.getName())
        .isEqualTo(VariantToBqUtils.ColumnKeyConstants.CALLS_SAMPLE_ID);
    assertThat(callsSampleIDField.getDescription())
        .isEqualTo(SchemaGeneratorImpl.FieldDescriptionConstants.CALLS_SAMPLE_ID);
    assertThat(callsSampleIDField.getMode()).isEqualTo(Field.Mode.NULLABLE);
    assertThat(callsSampleIDField.getType()).isEqualTo(LegacySQLTypeName.INTEGER);
    
    assertThat(callsGenotypeField.getName())
        .isEqualTo(VariantToBqUtils.ColumnKeyConstants.CALLS_GENOTYPE);
    assertThat(callsGenotypeField.getDescription())
        .isEqualTo(SchemaGeneratorImpl.FieldDescriptionConstants.CALLS_GENOTYPE);
    assertThat(callsGenotypeField.getMode()).isEqualTo(Field.Mode.REPEATED);
    assertThat(callsGenotypeField.getType()).isEqualTo(LegacySQLTypeName.INTEGER);

    assertThat(callsPhasesetField.getName())
        .isEqualTo(VariantToBqUtils.ColumnKeyConstants.CALLS_PHASESET);
    assertThat(callsPhasesetField.getDescription())
        .isEqualTo(SchemaGeneratorImpl.FieldDescriptionConstants.CALLS_PHASESET);
    assertThat(callsPhasesetField.getMode()).isEqualTo(Field.Mode.NULLABLE);
    assertThat(callsPhasesetField.getType()).isEqualTo(LegacySQLTypeName.STRING);
  }

  @Test
  //column: calls sample ID
  public void testCreateCallFields_whenGetCallsSampleIDField_thenIsEqualTo() {
    FieldList callFields = schemaGen.createCallFields();
    Field callsSampleIDField = callFields.get(CALLS_SAMPLE_ID_INDEX);

    assertThat(callsSampleIDField.getName())
        .isEqualTo(VariantToBqUtils.ColumnKeyConstants.CALLS_SAMPLE_ID);
    assertThat(callsSampleIDField.getDescription())
        .isEqualTo(SchemaGeneratorImpl.FieldDescriptionConstants.CALLS_SAMPLE_ID);
    assertThat(callsSampleIDField.getMode()).isEqualTo(Field.Mode.NULLABLE);
    assertThat(callsSampleIDField.getType()).isEqualTo(LegacySQLTypeName.INTEGER);
  }

  @Test
  //column: calls genotype
  public void testCreateCallFields_whenGetCallsGenotypeField_thenIsEqualTo() {
    FieldList callFields = schemaGen.createCallFields();
    Field callsGenotypeField = callFields.get(CALLS_GENOTYPE_INDEX);
    
    assertThat(callsGenotypeField.getName())
        .isEqualTo(VariantToBqUtils.ColumnKeyConstants.CALLS_GENOTYPE);
    assertThat(callsGenotypeField.getDescription())
        .isEqualTo(SchemaGeneratorImpl.FieldDescriptionConstants.CALLS_GENOTYPE);
    assertThat(callsGenotypeField.getMode()).isEqualTo(Field.Mode.REPEATED);
    assertThat(callsGenotypeField.getType()).isEqualTo(LegacySQLTypeName.INTEGER);
  }

  @Test
  //column: calls phaseset
  public void testCreateCallFields_whenGetCallsPhasesetField_thenIsEqualTo() {
    FieldList callFields = schemaGen.createCallFields();
    Field callsPhasesetField = callFields.get(CALLS_PHASESET_INDEX);

    assertThat(callsPhasesetField.getName())
        .isEqualTo(VariantToBqUtils.ColumnKeyConstants.CALLS_PHASESET);
    assertThat(callsPhasesetField.getDescription())
        .isEqualTo(SchemaGeneratorImpl.FieldDescriptionConstants.CALLS_PHASESET);
    assertThat(callsPhasesetField.getMode()).isEqualTo(Field.Mode.NULLABLE);
    assertThat(callsPhasesetField.getType()).isEqualTo(LegacySQLTypeName.STRING);
  }

}
