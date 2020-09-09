// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.gcp_variant_transforms.common.Constants;
import htsjdk.variant.vcf.VCFCompoundHeaderLine;
import htsjdk.variant.vcf.VCFConstants;
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
    return new TableSchema().setFields(schemaFields);
  }

  /**
   * Builds and returns a list of fields needed to create table schema.
   * @param vcfHeader
   * @return list of Field objects
   */
  @VisibleForTesting
  protected ImmutableList<TableFieldSchema> getFields(VCFHeader vcfHeader) {
    ImmutableList.Builder<TableFieldSchema> fields = new ImmutableList.Builder<>();
    Collection<String> constantFieldNames = SchemaUtils.constantFieldIndexToNameMap.values();

    // Adds constant fields and records.
    for (String constantFieldName : constantFieldNames) {
      TableFieldSchema field = createField(vcfHeader, constantFieldName);
      fields.add(field);
    }
    // Adds remaining INFO fields.
    for (VCFInfoHeaderLine infoHeaderLine : vcfHeader.getInfoHeaderLines()){
      // INFO header lines with Number = 'A' are added under the alternate bases record
      if (infoHeaderLine.getCountType() != VCFHeaderLineCount.A)
        fields.add(convertCompoundHeaderLineToField(infoHeaderLine, true));
    }

    return fields.build();
  }

  /**
   * Creates a single Field
   * @param vcfHeader
   * @param fieldName
   * @return field
   */
  @VisibleForTesting
  protected TableFieldSchema createField(VCFHeader vcfHeader, String fieldName){
    TableFieldSchema field;
    if (fieldName.equals(Constants.ColumnKeyNames.ALTERNATE_BASES) ||
        fieldName.equals(Constants.ColumnKeyNames.CALLS)) {
      field = createRecordField(vcfHeader, fieldName);
    } else {
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
    if (fieldName.equals(Constants.ColumnKeyNames.ALTERNATE_BASES)) {
      subFields = getAltSubFields(vcfHeader);
    } else { subFields = getCallSubFields(vcfHeader); } // Call record

    return new TableFieldSchema()
        .setName(fieldName)
        .setDescription(SchemaUtils.constantFieldNameToDescriptionMap.get(fieldName))
        .setMode(SchemaUtils.constantFieldNameToModeMap.get(fieldName))
        .setType(SchemaUtils.constantFieldNameToTypeMap.get(fieldName))
        .setFields(subFields);
  }

  /**
   * Creates and returns a Field from a compound header line
   * @param headerLine
   * @return an INFO or FORMAT Field
   */
  @VisibleForTesting
  protected TableFieldSchema convertCompoundHeaderLineToField(VCFCompoundHeaderLine headerLine,
                                                              boolean isInfo) {
    String bqFieldMode;
    if ((headerLine.getCountType() == VCFHeaderLineCount.INTEGER && headerLine.getCount() <= 1) ||
        ((headerLine.getCountType() == VCFHeaderLineCount.A) && isInfo)) {
      bqFieldMode = SchemaUtils.BQFieldMode.NULLABLE;
    } else { bqFieldMode = SchemaUtils.BQFieldMode.REPEATED; }   // Number = A, R, G, or >1
    return new TableFieldSchema()
        .setName(SchemaUtils.getSanitizedFieldName(headerLine.getID()))
        .setDescription(headerLine.getDescription())
        .setMode(bqFieldMode)
        .setType(SchemaUtils.HTSJDKTypeToBQTypeMap.get(headerLine.getType()));
  }

  /**
   * Creates and returns a Call record's subfields.
   * @param vcfHeader
   * @return a list of call sub-fields
   */
  @VisibleForTesting
  protected ImmutableList<TableFieldSchema> getCallSubFields(VCFHeader vcfHeader) {
    ImmutableList.Builder<TableFieldSchema> callSubFields =
        new ImmutableList.Builder<>();
    Collection<String> callFieldNames = SchemaUtils.callsSubFieldIndexToNameMap.values();
    for (String fieldName : callFieldNames) {
      TableFieldSchema callSubField = new TableFieldSchema()
          .setName(fieldName)
          .setDescription(SchemaUtils.callSubFieldNameToDescriptionMap.get(fieldName))
          .setMode(SchemaUtils.callSubFieldNameToModeMap.get(fieldName))
          .setType(SchemaUtils.callSubFieldNameToTypeMap.get(fieldName));
      callSubFields.add(callSubField);
    }
    // Adds the FORMAT fields under the Calls record
    for (VCFFormatHeaderLine formatHeaderLine : vcfHeader.getFormatHeaderLines()){
      // Phaseset and Genotype are already added to callSubFields
      String callSubFieldName = formatHeaderLine.getID();
      if (!callSubFieldName.equals(VCFConstants.GENOTYPE_KEY)
          && !callSubFieldName.equals(VCFConstants.PHASE_SET_KEY)) {
        callSubFields.add(convertCompoundHeaderLineToField(formatHeaderLine, false));
      }
    }
    return callSubFields.build();
  }

  /**
   * Creates and returns an Alternate Base record's subfields.
   * @param vcfHeader
   * @return a list of alternate base sub-fields
   */
  @VisibleForTesting
  protected ImmutableList<TableFieldSchema> getAltSubFields(VCFHeader vcfHeader) {
    ImmutableList.Builder<TableFieldSchema> altSubFields =
        new ImmutableList.Builder<>();
    // Adds the alternate bases allele column.
    TableFieldSchema altSubField = new TableFieldSchema()
        .setName(Constants.ColumnKeyNames.ALTERNATE_BASES_ALT)
        .setDescription(SchemaUtils.FieldDescription.ALTERNATE_BASES_ALT)
        .setMode(SchemaUtils.BQFieldMode.NULLABLE)
        .setType(SchemaUtils.BQFieldType.STRING);
    altSubFields.add(altSubField);
    // Adds the INFO fields with Number = A (i.e., one value for each alternate) among alternates.
    for (VCFInfoHeaderLine infoHeaderLine : vcfHeader.getInfoHeaderLines()){
      if (infoHeaderLine.getCountType() == VCFHeaderLineCount.A){
        altSubFields.add(convertCompoundHeaderLineToField(infoHeaderLine, true));
      }
    }
    return altSubFields.build();
  }
}
