// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.gcp_variant_transforms.common.Constants;
import htsjdk.variant.vcf.VCFHeader;
import java.util.Collection;

/**
 * Service to create BigQuery Schema from VCFHeader 
 */
public class SchemaGeneratorImpl implements SchemaGenerator {
  
  public TableSchema getSchema(VCFHeader vcfHeader){
    ImmutableList<TableFieldSchema> schemaFields = getFields(vcfHeader);
    TableSchema schema = new TableSchema()
        .setFields(schemaFields);
    return schema;
  }

  /**
   * Builds and returns a list of fields to create table schema.
   * @param vcfHeader
   * @return ImmutableList<TableFieldSchema>
   */
  @VisibleForTesting
  protected ImmutableList<TableFieldSchema> getFields(VCFHeader vcfHeader){
    ImmutableList.Builder<TableFieldSchema> fieldsBuilder = new ImmutableList.Builder<TableFieldSchema>();
    Collection<String> fieldNames = SchemaUtils.constantFieldIndexToNameMap.values();

    for (String fieldName : fieldNames){
      TableFieldSchema field;
      // Creating a Call record requires generating its sub-fields.
      if( fieldName == Constants.ColumnKeyNames.CALLS){
        ImmutableList<TableFieldSchema> callsSubFields = getCallSubFields();
        field = new TableFieldSchema()
            .setName(fieldName)
            .setMode(SchemaUtils.constantFieldNameToModeMap.get(fieldName))
            .setDescription(SchemaUtils.constantFieldNameToDescriptionMap.get(fieldName))
            .setFields(callsSubFields)
            .setType(SchemaUtils.constantFieldNameToTypeMap.get(fieldName));
      } else {
        field = new TableFieldSchema()
            .setName(fieldName)
            .setMode(SchemaUtils.constantFieldNameToModeMap.get(fieldName))
            .setDescription(SchemaUtils.constantFieldNameToDescriptionMap.get(fieldName))
            .setType(SchemaUtils.constantFieldNameToTypeMap.get(fieldName));
      }
      fieldsBuilder.add(field);
    }
    
    // Add formats

    // Add info fields
    
    return fieldsBuilder.build();
  }

  /**
   * Creates and returns Call record's subfields.
   * @return ImmutableList<TableFieldSchema>
   */
  @VisibleForTesting
  protected ImmutableList<TableFieldSchema> getCallSubFields(){
    ImmutableList.Builder<TableFieldSchema> callSubFieldsBuilder = new ImmutableList.Builder<TableFieldSchema>();
    Collection<String> fieldNames = SchemaUtils.callsSubFieldIndexToNameMap.values();
    for (String fieldName : fieldNames){
      TableFieldSchema callSubField = new TableFieldSchema()
            .setName(fieldName)
            .setMode(SchemaUtils.callSubFieldNameToModeMap.get(fieldName))
            .setDescription(SchemaUtils.callSubFieldNameToDescriptionMap.get(fieldName))
            .setType(SchemaUtils.callSubFieldNameToTypeMap.get(fieldName));
      callSubFieldsBuilder.add(callSubField);
    }
    return callSubFieldsBuilder.build();
  }
}
