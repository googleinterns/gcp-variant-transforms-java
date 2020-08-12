// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.collect.ImmutableList;
import htsjdk.variant.vcf.VCFHeader;



/**
 * Service to create BigQuery Schema from VCFHeader 
 */
public class SchemaGeneratorImpl implements SchemaGenerator {
  
  public Schema getSchema(VCFHeader vcfHeader){
    ImmutableList<Field> schemaFields = getFields(vcfHeader);
    Schema schema = Schema.of(schemaFields);
    return schema;
  }

  public ImmutableList<Field> getFields(VCFHeader vcfHeader){
    ImmutableList.Builder<Field> fields = new ImmutableList.Builder<Field>();
    Field fieldReferenceName = Field.newBuilder(
        VariantToBqUtils.ColumnKeyConstants.REFERENCE_NAME, StandardSQLTypeName.STRING)
        .setMode(Field.Mode.NULLABLE)
        .setDescription(FieldDescriptionConstants.REFERENCE_NAME)
        .build();

    fields.add(fieldReferenceName);

    Field fieldStartPosition = Field.newBuilder(
        VariantToBqUtils.ColumnKeyConstants.START_POSITION, StandardSQLTypeName.INT64)
        .setMode(Field.Mode.NULLABLE)
        .setDescription(FieldDescriptionConstants.START_POSITION)
        .build();
    fields.add(fieldStartPosition);

    Field fieldEndPosition = Field.newBuilder(
        VariantToBqUtils.ColumnKeyConstants.END_POSITION, StandardSQLTypeName.INT64)
        .setMode(Field.Mode.NULLABLE)
        .setDescription(FieldDescriptionConstants.END_POSITION)
        .build();
    fields.add(fieldEndPosition);

    Field fieldReferenceBases = Field.newBuilder(
        VariantToBqUtils.ColumnKeyConstants.REFERENCE_BASES, StandardSQLTypeName.STRING)
        .setMode(Field.Mode.NULLABLE)
        .setDescription(FieldDescriptionConstants.REFERENCE_BASES)
        .build();
    fields.add(fieldReferenceBases);

    Field fieldNames = Field.newBuilder(
        VariantToBqUtils.ColumnKeyConstants.NAMES, StandardSQLTypeName.STRING)
        .setMode(Field.Mode.REPEATED)
        .setDescription(FieldDescriptionConstants.NAMES)
        .build();
    fields.add(fieldNames);

    Field fieldQuality = Field.newBuilder(
      VariantToBqUtils.ColumnKeyConstants.QUALITY, StandardSQLTypeName.FLOAT64)
      .setMode(Field.Mode.NULLABLE)
      .setDescription(FieldDescriptionConstants.QUALITY)
      .build();
    fields.add(fieldQuality);

    Field fieldFilter = Field.newBuilder(
        VariantToBqUtils.ColumnKeyConstants.FILTER, StandardSQLTypeName.STRING)
        .setMode(Field.Mode.REPEATED)
        .setDescription(FieldDescriptionConstants.FILTER)
        .build();
    fields.add(fieldFilter);

    // Add calls
    FieldList callFields = createCallFields();
    Field fieldCalls = Field.newBuilder(
        VariantToBqUtils.ColumnKeyConstants.CALLS, StandardSQLTypeName.STRUCT, callFields)
        .setMode(Field.Mode.REPEATED)
        .setDescription(FieldDescriptionConstants.CALLS)
        .build();
    fields.add(fieldCalls);
    
    // Add formats

    // Add info fields
    
    return fields.build();
  }

  public FieldList createCallFields(){
    ImmutableList.Builder<Field> callFields = new ImmutableList.Builder<Field>();
    Field fieldCallsSampleID = Field.newBuilder(
        VariantToBqUtils.ColumnKeyConstants.CALLS_SAMPLE_ID, StandardSQLTypeName.INT64)
        .setMode(Field.Mode.NULLABLE)
        .setDescription(FieldDescriptionConstants.CALLS_SAMPLE_ID)
        .build();
    callFields.add(fieldCallsSampleID);

    Field fieldCallsGenotype = Field.newBuilder(
        VariantToBqUtils.ColumnKeyConstants.CALLS_GENOTYPE, StandardSQLTypeName.INT64)
        .setMode(Field.Mode.REPEATED)
        .setDescription(FieldDescriptionConstants.CALLS_GENOTYPE)
        .build();
    callFields.add(fieldCallsGenotype);

    Field fieldCallsPhaseset = Field.newBuilder(
        VariantToBqUtils.ColumnKeyConstants.CALLS_PHASESET, StandardSQLTypeName.STRING)
        .setMode(Field.Mode.NULLABLE)
        .setDescription(FieldDescriptionConstants.CALLS_PHASESET)
        .build();
    callFields.add(fieldCallsPhaseset);
    return FieldList.of(callFields.build());
  }
}
