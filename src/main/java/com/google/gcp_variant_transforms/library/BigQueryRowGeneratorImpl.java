// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gcp_variant_transforms.common.Constants;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base abstract class for BigQuery row generator.
 * The base class provides the common functionalities when generating BigQuery
 * row (e.g., sanitizing the BigQuery field, resolving the conflicts between the schema and data).
 * Derived classes must implement "get_rows".
 */
public class BigQueryRowGeneratorImpl implements BigQueryRowGenerator{

  public TableRow getRows(VariantContext variantContext, VCFHeader vcfHeader) {
    TableRow row = new TableRow();

    row.set(Constants.ColumnKeyConstants.REFERENCE_NAME, variantContext.getContig());
    row.set(Constants.ColumnKeyConstants.START_POSITION, variantContext.getStart());
    row.set(Constants.ColumnKeyConstants.END_POSITION, variantContext.getEnd());
    // To index the alleles in the GT field, we need to build a map to store all the alleles in
    // ref field and allele field
    Map<String, Integer> alleleIndexingMap = new HashMap<>();
    String reference = VariantToBqUtils.addReferenceBase(variantContext);
    alleleIndexingMap.put(reference, 0);
    row.set(Constants.ColumnKeyConstants.REFERENCE_BASES, reference);
    row.set(Constants.ColumnKeyConstants.NAMES, VariantToBqUtils.addNames(variantContext));

    // write alt field and info field to BQ row
    List<TableRow> altMetadata = VariantToBqUtils.addAlternates(variantContext, alleleIndexingMap);
    // altMeta should add those field that info number = 'A', then add to row
    // Other fields will directly add to the base BQ row
    VariantToBqUtils.addInfo(row, variantContext, altMetadata, vcfHeader);
    row.set(Constants.ColumnKeyConstants.ALTERNATE_BASES, altMetadata);

    row.set(Constants.ColumnKeyConstants.QUALITY, variantContext.getPhredScaledQual());
    row.set(Constants.ColumnKeyConstants.FILTER, VariantToBqUtils.addFilters(variantContext));

    // write calls to BQ row
    GenotypesContext genotypesContext = variantContext.getGenotypes();
    List<TableRow> callRows = VariantToBqUtils.addCalls(genotypesContext, vcfHeader,
            alleleIndexingMap);
    row.set(Constants.ColumnKeyConstants.CALLS, callRows);

    return row;
  }
}
