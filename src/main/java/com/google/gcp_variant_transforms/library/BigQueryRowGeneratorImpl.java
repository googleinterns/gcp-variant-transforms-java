// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gcp_variant_transforms.common.Constants;
import com.google.gcp_variant_transforms.exceptions.MalformedRecordException;
import com.google.inject.Inject;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import java.io.Serializable;
import java.util.List;

/**
 * Implementation class for BigQuery row generator. The class provides functionality to generate
 * a {@link TableRow} and set table row field value from {@link VariantContext}. Each value in
 * the table field should match the format in {@link VCFHeader}.
 */
public class BigQueryRowGeneratorImpl implements BigQueryRowGenerator, Serializable {

  @Inject
  VariantToBqUtils variantToBqUtils;

  public TableRow convertToBQRow(VariantContext variantContext, VCFHeader vcfHeader) {
    TableRow row = new TableRow();

    row.set(Constants.ColumnKeyNames.REFERENCE_NAME, variantContext.getContig());
    row.set(Constants.ColumnKeyNames.START_POSITION, variantContext.getStart());
    row.set(Constants.ColumnKeyNames.END_POSITION, variantContext.getEnd());

    row.set(Constants.ColumnKeyNames.REFERENCE_BASES,
        variantToBqUtils.getReferenceBases(variantContext));
    try {
      row.set(Constants.ColumnKeyNames.NAMES, variantToBqUtils.getNames(variantContext));

      // Write alt field and info field to BQ row.
      List<TableRow> altMetadata = variantToBqUtils.getAlternateBases(variantContext);
      // AltMetadata should contain Info fields with Number='A' tag, then be added to the row.
      // The rest of Info fields will be directly added to the base BQ row.
      variantToBqUtils.addInfo(row, variantContext, altMetadata, vcfHeader, 2);
      row.set(Constants.ColumnKeyNames.ALTERNATE_BASES, altMetadata);

      row.set(Constants.ColumnKeyNames.QUALITY, variantContext.getPhredScaledQual());
      row.set(Constants.ColumnKeyNames.FILTER, variantToBqUtils.getFilters(variantContext));

      // Write calls to BQ row.
      List<TableRow> callRows = variantToBqUtils.getCalls(variantContext, vcfHeader);
      row.set(Constants.ColumnKeyNames.CALLS, callRows);
    } catch (Exception e) {
      throw new MalformedRecordException(e.getMessage(), variantContext.getContig(),
          String.valueOf(variantContext.getStart()),
          variantToBqUtils.getReferenceBases(variantContext));
    }
    return row;
  }
}
