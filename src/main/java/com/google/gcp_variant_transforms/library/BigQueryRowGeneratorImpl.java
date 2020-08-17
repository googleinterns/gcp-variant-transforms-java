// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gcp_variant_transforms.common.Constants;
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

  public TableRow convertToBQRow(VariantContext variantContext, VCFHeader vcfHeader) {
    TableRow row = new TableRow();

    row.set(Constants.ColumnKeyConstants.REFERENCE_NAME, variantContext.getContig());
    row.set(Constants.ColumnKeyConstants.START_POSITION, variantContext.getStart());
    row.set(Constants.ColumnKeyConstants.END_POSITION, variantContext.getEnd());

    row.set(Constants.ColumnKeyConstants.REFERENCE_BASES,
        VariantToBqUtils.getReferenceBases(variantContext));
    row.set(Constants.ColumnKeyConstants.NAMES, VariantToBqUtils.getNames(variantContext));

    // Write alt field and info field to BQ row.
    List<TableRow> altMetadata = VariantToBqUtils.getAlternateBases(variantContext);
    // AltMetadata should contain Info fields with Number='A' tag, then be added to the row.
    // The rest of Info fields will be directly added to the base BQ row.
    VariantToBqUtils.addInfo(row, variantContext, altMetadata, vcfHeader);
    row.set(Constants.ColumnKeyConstants.ALTERNATE_BASES, altMetadata);

    row.set(Constants.ColumnKeyConstants.QUALITY, variantContext.getPhredScaledQual());
    row.set(Constants.ColumnKeyConstants.FILTER, VariantToBqUtils.getFilters(variantContext));

    // Write calls to BQ row.
    List<TableRow> callRows = VariantToBqUtils.addCalls(variantContext, vcfHeader);
    row.set(Constants.ColumnKeyConstants.CALLS, callRows);

    return row;
  }
}
