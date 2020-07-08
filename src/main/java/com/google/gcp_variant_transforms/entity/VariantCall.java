// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.entity;

import java.util.List;
import java.util.Map;

/**
 * A class to store info about a variant call.
 * A call represents the determination of genotype with respect to a particular
 * variant. It may include associated information such as quality and phasing.
 */
public class VariantCall {

  private String sampleName;
  private String phaseSet;
  private List<Integer> genotype;
  private Map<String, Object> info;

  /**
   * @param sampleName   The name of the Variant Call
   * @param genotype   The genotype of this variant call as specified by the VCF schema.
   *                   The values are either `0` representing the reference, or a 1-based index
   *                   into alternate bases.
   *                   Ordering is only important if `phaseset` is present. If a genotype is not
   *                   called (that is, a `.` is present in the GT string), -1 is used.
   * @param phaseSet   If this field is present, this variant call's genotype ordering implies
   *                   the phase of the bases and is consistent with any other variant calls in
   *                   the same reference sequence which have the same phaseset value. If the
   *                   genotype data was phased but no phase set was specified, this field will
   *                   be set to `*`.
   * @param info       A map of additional variant call information. The key is specified in the
   *                   VCF record and the type of the value is specified by the VCF header FORMAT.
   */
  public VariantCall(String sampleName, List<Integer> genotype,
                     String phaseSet, Map<String, Object> info) {
    this.sampleName = sampleName;
    this.genotype = genotype;
    this.phaseSet = phaseSet;
    this.info = info;
  }

  public String getSampleName() {
    return sampleName;
  }

  public String getPhaseSet() {
    return phaseSet;
  }

  public List<Integer> getGenotype() {
    return genotype;
  }

  public Map<String, Object> getInfo() {
    return info;
  }
}
