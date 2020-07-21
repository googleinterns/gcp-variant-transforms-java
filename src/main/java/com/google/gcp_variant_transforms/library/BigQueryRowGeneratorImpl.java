// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.api.services.bigquery.model.TableRow;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import java.util.List;
import java.util.Set;

/**
 * Base abstract class for BigQuery row generator.
 * The base class provides the common functionalities when generating BigQuery
 * row (e.g., sanitizing the BigQuery field, resolving the conflicts between the schema and data).
 * Derived classes must implement "get_rows".
 */
public class BigQueryRowGeneratorImpl implements BigQueryRowGenerator{

  public TableRow getRows(VariantContext variantContext, VCFHeader vcfHeader) {
    TableRow row = new TableRow();

    row.set(VariantToBqUtils.ColumnKeyConstants.REFERENCE_NAME, variantContext.getContig());
    row.set(VariantToBqUtils.ColumnKeyConstants.START_POSITION, variantContext.getStart());
    row.set(VariantToBqUtils.ColumnKeyConstants.END_POSITION, variantContext.getEnd());
    row.set(VariantToBqUtils.ColumnKeyConstants.REFERENCE_BASES,
            VariantToBqUtils.buildReferenceBase(variantContext));
    row.set(VariantToBqUtils.ColumnKeyConstants.NAMES, variantContext.getID());

    // write alt field and info field to BQ row
    List<TableRow> altMetadata = VariantToBqUtils.addAlternates(variantContext);
    // altMeta should add those field that info number = 'A', then add to row
    // Other fields will directly add to the base BQ row
    VariantToBqUtils.addInfo(row, variantContext, altMetadata, vcfHeader);
    row.set(VariantToBqUtils.ColumnKeyConstants.ALTERNATE_BASES, altMetadata);

    row.set(VariantToBqUtils.ColumnKeyConstants.QUALITY, variantContext.getPhredScaledQual());

    Set<String> filters = variantContext.getFilters();
    if (filters.isEmpty()) {
      filters.add("PASS");
    }
    row.set(VariantToBqUtils.ColumnKeyConstants.FILTER, filters);

    // write calls to BQ row
    GenotypesContext genotypesContext = variantContext.getGenotypes();
    List<TableRow> callRows = VariantToBqUtils.addCalls(genotypesContext, vcfHeader);
    row.set(VariantToBqUtils.ColumnKeyConstants.CALLS, callRows);

    return row;
  }
}
