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
   * @return list of Field objects
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
            .setDescription(SchemaUtils.constantFieldNameToDescriptionMap.get(fieldName))
            .setMode(SchemaUtils.constantFieldNameToModeMap.get(fieldName))
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
        .setDescription(SchemaUtils.constantFieldNameToDescriptionMap.get(fieldName))
        .setMode(SchemaUtils.constantFieldNameToModeMap.get(fieldName))
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
        .setName(getSanitizedFieldName(infoHeaderLine.getID()))
        .setDescription(infoHeaderLine.getDescription())
        .setMode(SchemaUtils.BQFieldMode.NULLABLE) // Always NULLABLE
        .setType(SchemaUtils.HTSJDKTypeToBQTypeMap.get(infoHeaderLine.getType()));
  }

  /**
   * Creates and returns a Call record's subfields.
   * @param vcfHeader
   * @return a list of call sub-fields
   */
  @VisibleForTesting
  protected ImmutableList<TableFieldSchema> getCallSubFields(VCFHeader vcfHeader) {
    ImmutableList.Builder<TableFieldSchema> callSubFieldsBuilder = new ImmutableList.Builder<TableFieldSchema>();
    Collection<String> fieldNames = SchemaUtils.callsSubFieldIndexToNameMap.values();
    for (String fieldName : fieldNames) {
      TableFieldSchema callSubField = new TableFieldSchema()
          .setName(fieldName)
          .setDescription(SchemaUtils.callSubFieldNameToDescriptionMap.get(fieldName))
          .setMode(SchemaUtils.callSubFieldNameToModeMap.get(fieldName))
          .setType(SchemaUtils.callSubFieldNameToTypeMap.get(fieldName));
      callSubFieldsBuilder.add(callSubField);
    }
    // Adds the FORMAT fields under the Calls record
    for (VCFFormatHeaderLine formatHeaderLine : vcfHeader.getFormatHeaderLines()){
        TableFieldSchema formatField = new TableFieldSchema()
            .setName(getSanitizedFieldName(formatHeaderLine.getID()))
            .setDescription(formatHeaderLine.getDescription())
            .setMode(SchemaUtils.BQFieldMode.NULLABLE) // Always NULLABLE
            .setType(SchemaUtils.HTSJDKTypeToBQTypeMap.get(formatHeaderLine.getType()));
        callSubFieldsBuilder.add(formatField);
      }
    return callSubFieldsBuilder.build();
  }

  /**
   * Creates and returns an Alternate Base record's subfields.
   * @param vcfHeader
   * @return a list of alternate base sub-fields
   */
  @VisibleForTesting
  protected ImmutableList<TableFieldSchema> getAltBaseSubFields(VCFHeader vcfHeader) {
    ImmutableList.Builder<TableFieldSchema> altBaseSubFieldsBuilder = new ImmutableList.Builder<TableFieldSchema>();
    // Adds the alternate bases allele column.
    TableFieldSchema altSubField = new TableFieldSchema()
        .setName(Constants.ColumnKeyNames.ALTERNATE_BASES_ALT)
        .setDescription(SchemaUtils.FieldDescription.ALTERNATE_BASES_ALT)
        .setMode(SchemaUtils.BQFieldMode.NULLABLE)
        .setType(SchemaUtils.BQFieldType.STRING);
    altBaseSubFieldsBuilder.add(altSubField);
    // Adds the INFO fields with Number = A (i.e., one value for each alternate) among alternates.
    for (VCFInfoHeaderLine infoHeaderLine : vcfHeader.getInfoHeaderLines()){
      if (infoHeaderLine.getCountType() == VCFHeaderLineCount.A){
        TableFieldSchema infoSubField = new TableFieldSchema()
            .setName(getSanitizedFieldName(infoHeaderLine.getID()))
            .setDescription(infoHeaderLine.getDescription())
            .setMode(SchemaUtils.BQFieldMode.NULLABLE) // Always NULLABLE
            .setType(SchemaUtils.HTSJDKTypeToBQTypeMap.get(infoHeaderLine.getType()));
        altBaseSubFieldsBuilder.add(infoSubField);
      }
    }
    return altBaseSubFieldsBuilder.build();
  }

  /**
   * Returns the sanitized field name according to BigQuery restrictions.
   *
   * BigQuery field names must follow `[a-zA-Z][a-zA-Z0-9_]*`. This method
   * converts any unsupported characters to an underscore. Also, if the first
   * character does not match `[a-zA-z]`, it prepends `FALLBACK_FIELD_NAME_PREFIX`
   * to the name.
   * @param fieldName: Name of the field to sanitize
   * @return sanitized field name
   */
  protected String getSanitizedFieldName(String fieldName){
    if (!Character.isLetter(fieldName.charAt(0)))
      fieldName = SchemaUtils.FALLBACK_FIELD_NAME_PREFIX + fieldName;
    return fieldName.replaceAll("[^a-zA-Z0-9_]", "_");
  }
}
