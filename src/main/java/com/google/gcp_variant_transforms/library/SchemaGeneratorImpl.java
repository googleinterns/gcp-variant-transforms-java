// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import htsjdk.variant.vcf.VCFHeader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

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

  public ImmutableList<TableFieldSchema> getFields(VCFHeader vcfHeader){
    ImmutableList.Builder<TableFieldSchema> fields = new ImmutableList.Builder<TableFieldSchema>();
    Integer[] fieldIndices = getFieldIndices(SchemaUtils.constantFieldIndexToNameMap);

    for (Integer fieldIndex : fieldIndices){
      String fieldName = SchemaUtils.constantFieldIndexToNameMap.get(fieldIndex);
      TableFieldSchema field;
      if( fieldName == SchemaUtils.FieldName.CALLS){ // requires generating sub-fields
        ImmutableList<TableFieldSchema> callsSubFields = createCallSubFields();
        field = new TableFieldSchema()
            .setName(fieldName)
            .setMode(SchemaUtils.constantFieldNameToModeMap.get(fieldName))
            .setDescription(SchemaUtils.constantFieldNameToDescriptionMap.get(fieldName))
            .setFields(callsSubFields)
            .setType(SchemaUtils.constantFieldNameToTypeMap.get(fieldName));
      }else {
        field = new TableFieldSchema()
            .setName(fieldName)
            .setMode(SchemaUtils.constantFieldNameToModeMap.get(fieldName))
            .setDescription(SchemaUtils.constantFieldNameToDescriptionMap.get(fieldName))
            .setType(SchemaUtils.constantFieldNameToTypeMap.get(fieldName));
      }
      fields.add(field);
    }
    
    // Add formats

    // Add info fields
    
    return fields.build();
  }

  public ImmutableList<TableFieldSchema> createCallSubFields(){
    ImmutableList.Builder<TableFieldSchema> callSubFieldsBuilder = new ImmutableList.Builder<TableFieldSchema>();
    Integer[] fieldIndices = getFieldIndices(SchemaUtils.callsSubFieldIndexToNameMap);
    for (Integer fieldIndex : fieldIndices){
      String fieldName = SchemaUtils.callsSubFieldIndexToNameMap.get(fieldIndex);
      TableFieldSchema callSubField = new TableFieldSchema()
            .setName(fieldName)
            .setMode(SchemaUtils.callSubFieldNameToModeMap.get(fieldName))
            .setDescription(SchemaUtils.callSubFieldNameToDescriptionMap.get(fieldName))
            .setType(SchemaUtils.callSubFieldNameToTypeMap.get(fieldName));
      callSubFieldsBuilder.add(callSubField);
    }
    return callSubFieldsBuilder.build();
  }

  // Retrieves field indices for a specific field list
  // i.e. calls fields or constant fields
  public Integer[] getFieldIndices(Map<Integer, String> indexToNameMap){
    Integer[] fieldIndices = indexToNameMap.keySet()
        .toArray(new Integer[indexToNameMap.size()]);
    Arrays.sort(fieldIndices);
    return fieldIndices;
  }
}
