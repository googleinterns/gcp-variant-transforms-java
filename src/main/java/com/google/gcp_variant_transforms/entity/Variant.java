// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.entity;

import htsjdk.variant.variantcontext.VariantContext;
import java.io.Serializable;

/**
 * Internal representation of Variant Data.
 */
public class Variant implements Serializable {
  private static final long serialVersionUID = 260660913763642347L;

  private final String contig;
    private final int start;
    private final int end;

    public Variant(String contig, int start, int end) {
      // TODO: extend to full representation of Variant row.
      this.contig = contig;
      this.start = start;
      this.end = end;
    }

    public Variant(VariantContext variantContext) {
      this(variantContext.getContig(), variantContext.getStart(), variantContext.getEnd());
    }

    public String getContig() {
      return contig;
    }

    public int getStart() {
      return start;
    }

    public int getEnd() {
      return end;
    }
}
