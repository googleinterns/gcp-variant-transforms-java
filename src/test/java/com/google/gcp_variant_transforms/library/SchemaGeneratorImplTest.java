// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableListIterator;
import com.google.guiceberry.junit4.GuiceBerryRule;
import com.google.gcp_variant_transforms.TestEnv;
import com.google.inject.Inject;
import htsjdk.variant.vcf.VCFHeader;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.xml.validation.Schema;

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
  //column: reference field
  public void testGetFields_whenGetReferenceField_thenIsEqualTo() {
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.REFERENCE_NAME);
    TableFieldSchema referenceField = fieldIterator.next();

    assertThat(referenceField.getName())
        .isEqualTo(SchemaUtils.FieldName.REFERENCE_NAME);
    assertThat(referenceField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.REFERENCE_NAME);
    assertThat(referenceField.getMode()).isEqualTo(SchemaUtils.FieldMode.NULLABLE);
    assertThat(referenceField.getType()).isEqualTo(SchemaUtils.FieldType.STRING);
  }

  @Test
  //column: start position
  public void testGetFields_whenGetStartPosField_thenIsEqualTo() {
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.START_POSITION);
    TableFieldSchema startPosField = fieldIterator.next();

    assertThat(startPosField.getName())
        .isEqualTo(SchemaUtils.FieldName.START_POSITION);
    assertThat(startPosField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.START_POSITION);
    assertThat(startPosField.getMode()).isEqualTo(SchemaUtils.FieldMode.NULLABLE);
    assertThat(startPosField.getType()).isEqualTo(SchemaUtils.FieldType.INTEGER);
  }

  @Test
  //column: end position
  public void testGetFields_whenGetEndPosField_thenIsEqualTo() {
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.END_POSITION);
    TableFieldSchema endPosField = fieldIterator.next();

    assertThat(endPosField.getName())
        .isEqualTo(VariantToBqUtils.ColumnKeyConstants.END_POSITION);
    assertThat(endPosField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.END_POSITION);
    assertThat(endPosField.getMode()).isEqualTo(SchemaUtils.FieldMode.NULLABLE);
    assertThat(endPosField.getType()).isEqualTo(SchemaUtils.FieldType.INTEGER);
  }

  @Test
  //column: reference bases
  public void testGetFields_whenGetReferenceBasesField_thenIsEqualTo() {
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.REFERENCE_BASES);
    TableFieldSchema referenceBasesField = fieldIterator.next();

    assertThat(referenceBasesField.getName())
        .isEqualTo(SchemaUtils.FieldName.REFERENCE_BASES);
    assertThat(referenceBasesField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.REFERENCE_BASES);
    assertThat(referenceBasesField.getMode()).isEqualTo(SchemaUtils.FieldMode.NULLABLE);
    assertThat(referenceBasesField.getType()).isEqualTo(SchemaUtils.FieldType.STRING);
  }

  @Test
  //column: names
  public void testGetFields_whenGetNamesField_thenIsEqualTo() {
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.NAMES);
    TableFieldSchema namesField = fieldIterator.next();

    assertThat(namesField.getName())
        .isEqualTo(SchemaUtils.FieldName.NAMES);
    assertThat(namesField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.NAMES);
    assertThat(namesField.getMode()).isEqualTo(SchemaUtils.FieldMode.REPEATED);
    assertThat(namesField.getType()).isEqualTo(SchemaUtils.FieldType.STRING);
  }

  @Test
  //column: quality
  public void testGetFields_whenGetQualityField_thenIsEqualTo() {
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.QUALITY);
    TableFieldSchema qualityField = fieldIterator.next();

    assertThat(qualityField.getName())
        .isEqualTo(SchemaUtils.FieldName.QUALITY);
    assertThat(qualityField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.QUALITY);
    assertThat(qualityField.getMode()).isEqualTo(SchemaUtils.FieldMode.NULLABLE);
    assertThat(qualityField.getType()).isEqualTo(SchemaUtils.FieldType.FLOAT);
  }

  @Test
  //column: filter
  public void testGetFields_whenGetFilterField_thenIsEqualTo() {
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.FILTER);
    TableFieldSchema filterField = fieldIterator.next();

    assertThat(filterField.getName())
        .isEqualTo(SchemaUtils.FieldName.FILTER);
    assertThat(filterField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.FILTER);
    assertThat(filterField.getMode()).isEqualTo(SchemaUtils.FieldMode.REPEATED);
    assertThat(filterField.getType()).isEqualTo(SchemaUtils.FieldType.STRING);
  }

  @Test
  //column: calls record
  public void testGetFields_whenGetCallsField_thenIsEqualTo() {
    ImmutableList<TableFieldSchema> fields = schemaGen.getFields(vcfHeader);
    UnmodifiableListIterator<TableFieldSchema> fieldIterator =
        fields.listIterator(SchemaUtils.FieldIndex.CALLS);
    TableFieldSchema callsField = fieldIterator.next();

    assertThat(callsField.getName())
        .isEqualTo(SchemaUtils.FieldName.CALLS);
    assertThat(callsField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.CALLS);
    assertThat(callsField.getMode()).isEqualTo(SchemaUtils.FieldMode.REPEATED);
    assertThat(callsField.getType()).isEqualTo(SchemaUtils.FieldType.RECORD);
  }

  @Test
  //column: calls record's subfields
  // verifies that the call record's sub-Fields maintain data integrity 
  // after they are added to the call record in a FieldList 
  public void testGetFields_whenGetCallsSubFields_thenIsEqualTo() {
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

    assertThat(callsSampleNameField.getName())
        .isEqualTo(SchemaUtils.FieldName.CALLS_SAMPLE_NAME);
    assertThat(callsSampleNameField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.CALLS_SAMPLE_NAME);
    assertThat(callsSampleNameField.getMode()).isEqualTo(SchemaUtils.FieldMode.NULLABLE);
    assertThat(callsSampleNameField.getType()).isEqualTo(SchemaUtils.FieldType.STRING);
    
    assertThat(callsGenotypeField.getName())
        .isEqualTo(SchemaUtils.FieldName.CALLS_GENOTYPE);
    assertThat(callsGenotypeField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.CALLS_GENOTYPE);
    assertThat(callsGenotypeField.getMode()).isEqualTo(SchemaUtils.FieldMode.REPEATED);
    assertThat(callsGenotypeField.getType()).isEqualTo(SchemaUtils.FieldType.INTEGER);

    assertThat(callsPhasesetField.getName())
        .isEqualTo(SchemaUtils.FieldName.CALLS_PHASESET);
    assertThat(callsPhasesetField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.CALLS_PHASESET);
    assertThat(callsPhasesetField.getMode()).isEqualTo(SchemaUtils.FieldMode.NULLABLE);
    assertThat(callsPhasesetField.getType()).isEqualTo(SchemaUtils.FieldType.STRING);
  }

  @Test
  //column: calls sample Name
  public void testCreateCallFields_whenGetCallsSampleNameField_thenIsEqualTo() {
    ImmutableList<TableFieldSchema> callFields = schemaGen.createCallSubFields();
    TableFieldSchema callsSampleNameField = callFields
        .get(SchemaUtils.FieldIndex.CALLS_SAMPLE_NAME);

    assertThat(callsSampleNameField.getName())
        .isEqualTo(SchemaUtils.FieldName.CALLS_SAMPLE_NAME);
    assertThat(callsSampleNameField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.CALLS_SAMPLE_NAME);
    assertThat(callsSampleNameField.getMode()).isEqualTo(SchemaUtils.FieldMode.NULLABLE);
    assertThat(callsSampleNameField.getType()).isEqualTo(SchemaUtils.FieldType.STRING);
  }

  @Test
  //column: calls genotype
  public void testCreateCallFields_whenGetCallsGenotypeField_thenIsEqualTo() {
    ImmutableList<TableFieldSchema> callFields = schemaGen.createCallSubFields();
    TableFieldSchema callsGenotypeField = callFields
        .get(SchemaUtils.FieldIndex.CALLS_GENOTYPE);
    
    assertThat(callsGenotypeField.getName())
        .isEqualTo(SchemaUtils.FieldName.CALLS_GENOTYPE);
    assertThat(callsGenotypeField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.CALLS_GENOTYPE);
    assertThat(callsGenotypeField.getMode()).isEqualTo(SchemaUtils.FieldMode.REPEATED);
    assertThat(callsGenotypeField.getType()).isEqualTo(SchemaUtils.FieldType.INTEGER);
  }

  @Test
  //column: calls phaseset
  public void testCreateCallFields_whenGetCallsPhasesetField_thenIsEqualTo() {
    ImmutableList<TableFieldSchema> callFields = schemaGen.createCallSubFields();
    TableFieldSchema callsPhasesetField = callFields
        .get(SchemaUtils.FieldIndex.CALLS_PHASESET);

    assertThat(callsPhasesetField.getName())
        .isEqualTo(SchemaUtils.FieldName.CALLS_PHASESET);
    assertThat(callsPhasesetField.getDescription())
        .isEqualTo(SchemaUtils.FieldDescription.CALLS_PHASESET);
    assertThat(callsPhasesetField.getMode()).isEqualTo(SchemaUtils.FieldMode.NULLABLE);
    assertThat(callsPhasesetField.getType()).isEqualTo(SchemaUtils.FieldType.STRING);
  }

}
