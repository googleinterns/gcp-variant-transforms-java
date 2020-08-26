// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.gcp_variant_transforms.common.Constants;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import java.util.Collection;

/**
 * Service to create BigQuery Schema from VCFHeader 
 */
public class SchemaGeneratorImpl implements SchemaGenerator {
  
  public TableSchema getSchema(VCFHeader vcfHeader) {
    ImmutableList<TableFieldSchema> schemaFields = getFields(vcfHeader);
    TableSchema schema = new TableSchema()
        .setFields(schemaFields);
    return schema;
  }

  /**
   * Builds and returns a list of fields needed to create table schema.
   * @param vcfHeader
   * @return ImmutableList<TableFieldSchema>
   */
  @VisibleForTesting
  protected ImmutableList<TableFieldSchema> getFields(VCFHeader vcfHeader) {
    ImmutableList.Builder<TableFieldSchema> fieldsBuilder = new ImmutableList.Builder<TableFieldSchema>();
    Collection<String> constantFieldNames = SchemaUtils.constantFieldIndexToNameMap.values();

    // Adds constant fields and records.
    for (String constantFieldName : constantFieldNames) {
      TableFieldSchema field = createField(vcfHeader, constantFieldName);
      fieldsBuilder.add(field);
    }

    // Adds remaining INFO fields.
    for (VCFInfoHeaderLine infoHeaderLine : vcfHeader.getInfoHeaderLines()){
      // INFO header lines with Number = 'A' are added under the alternate bases record
      if (infoHeaderLine.getCountType() != VCFHeaderLineCount.A)
        fieldsBuilder.add(createInfoField(infoHeaderLine));
    }
    
    return fieldsBuilder.build();
  }

  /**
   * Creates a single Field
   * @param vcfHeader
   * @param fieldName
   * @return field
   */
  protected TableFieldSchema createField(VCFHeader vcfHeader, String fieldName){
    TableFieldSchema field;
    switch(fieldName) {
      // Call records require generating the record's sub-fields.
      case(Constants.ColumnKeyNames.CALLS):
        field = createRecordField(vcfHeader, Constants.ColumnKeyNames.CALLS);
        break;
      // Alternate Base records require generating the record's sub-fields.
      case(Constants.ColumnKeyNames.ALTERNATE_BASES):
        field = createRecordField(vcfHeader, Constants.ColumnKeyNames.ALTERNATE_BASES);
        break;
      default: // All other fields, which are not records.
        field = new TableFieldSchema()
            .setName(fieldName)
            .setMode(SchemaUtils.constantFieldNameToModeMap.get(fieldName))
            .setDescription(SchemaUtils.constantFieldNameToDescriptionMap.get(fieldName))
            .setType(SchemaUtils.constantFieldNameToTypeMap.get(fieldName));
    }
    return field;
  }

  /**
   * Creates and returns a Field of type Record
   * @param vcfHeader
   * @param fieldName
   * @return a record field
   */
  @VisibleForTesting
  protected TableFieldSchema createRecordField(VCFHeader vcfHeader, String fieldName) {
    ImmutableList<TableFieldSchema> subFields;
    if (fieldName == Constants.ColumnKeyNames.ALTERNATE_BASES)
       subFields = getAltBaseSubFields(vcfHeader);
    else subFields = getCallSubFields(vcfHeader); // Call record

    return new TableFieldSchema()
        .setName(fieldName)
        .setMode(SchemaUtils.constantFieldNameToModeMap.get(fieldName))
        .setDescription(SchemaUtils.constantFieldNameToDescriptionMap.get(fieldName))
        .setFields(subFields)
        .setType(SchemaUtils.constantFieldNameToTypeMap.get(fieldName));
  }

  /**
   * Creates and returns an INFO Field
   * @param infoHeaderLine
   * @return an info Field
   */
  @VisibleForTesting
  protected TableFieldSchema createInfoField(VCFInfoHeaderLine infoHeaderLine) {
    return new TableFieldSchema()
        .setName(infoHeaderLine.getID())
        .setMode(SchemaUtils.BQFieldMode.NULLABLE) // Always NULLABLE
        .setDescription(infoHeaderLine.getDescription())
        .setType(SchemaUtils.HTSJDKTypeToBQTypeMap.get(infoHeaderLine.getType()));
  }

  /**
   * Creates and returns Call record's subfields.
   * @return a list of call subfields
   */
  @VisibleForTesting
  protected ImmutableList<TableFieldSchema> getCallSubFields(VCFHeader vcfHeader) {
    ImmutableList.Builder<TableFieldSchema> callSubFieldsBuilder = new ImmutableList.Builder<TableFieldSchema>();
    Collection<String> fieldNames = SchemaUtils.callsSubFieldIndexToNameMap.values();
    for (String fieldName : fieldNames) {
      TableFieldSchema callSubField = new TableFieldSchema()
            .setName(fieldName)
            .setMode(SchemaUtils.callSubFieldNameToModeMap.get(fieldName))
            .setDescription(SchemaUtils.callSubFieldNameToDescriptionMap.get(fieldName))
            .setType(SchemaUtils.callSubFieldNameToTypeMap.get(fieldName));
      callSubFieldsBuilder.add(callSubField);
    }
    // Adds the FORMAT fields under the Calls record
    for (VCFFormatHeaderLine formatHeaderLine : vcfHeader.getFormatHeaderLines()){
        TableFieldSchema formatField = new TableFieldSchema()
            .setName(formatHeaderLine.getID())
            .setMode(SchemaUtils.BQFieldMode.NULLABLE) // Always NULLABLE
            .setType(SchemaUtils.HTSJDKTypeToBQTypeMap.get(formatHeaderLine.getType()));
        callSubFieldsBuilder.add(formatField);
      }
    return callSubFieldsBuilder.build();
  }

  /**
   * Creates and returns Alternate Base record's subfields.
   * @return a list of alternate base subfields
   */
  @VisibleForTesting
  protected ImmutableList<TableFieldSchema> getAltBaseSubFields(VCFHeader vcfHeader) {
    ImmutableList.Builder<TableFieldSchema> altBaseSubFieldsBuilder = new ImmutableList.Builder<TableFieldSchema>();
    // Adds the alternate bases allele column.
    TableFieldSchema altSubField = new TableFieldSchema() // hard coded... ????
        .setName(Constants.ColumnKeyNames.ALTERNATE_BASES_ALT)
        .setMode(SchemaUtils.BQFieldMode.NULLABLE)
        .setType(SchemaUtils.BQFieldType.STRING);
    altBaseSubFieldsBuilder.add(altSubField);
    // Adds the INFO fields with Number = A (i.e., one value for each alternate) among alternates.
    for (VCFInfoHeaderLine infoHeaderLine : vcfHeader.getInfoHeaderLines()){
      if (infoHeaderLine.getCountType() == VCFHeaderLineCount.A){
        TableFieldSchema infoSubField = new TableFieldSchema()
            .setName(infoHeaderLine.getID())
            .setMode(SchemaUtils.BQFieldMode.NULLABLE) // Always NULLABLE
            .setType(SchemaUtils.HTSJDKTypeToBQTypeMap.get(infoHeaderLine.getType()));
        altBaseSubFieldsBuilder.add(infoSubField);
      }
    }
    return altBaseSubFieldsBuilder.build();
  }
}
