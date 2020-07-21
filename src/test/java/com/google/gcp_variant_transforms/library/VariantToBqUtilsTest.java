// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import static com.google.common.truth.Truth.assertThat;

import htsjdk.variant.vcf.VCFHeaderLineType;
import org.junit.Test;

import java.util.Arrays;

/**
 * Units tests for VariantToBqUtils.java
 */
public class VariantToBqUtilsTest {

  @Test
  public void testConvertStringValueToRightValueType_whenComparingElement_thenTrue() {
    // test list of values
    String integerListStr = "23,27";
    assertThat(VariantToBqUtils.convertToDefinedType(integerListStr, VCFHeaderLineType.Integer))
            .isEqualTo(Arrays.asList(23, 27));
    String floatListStr = "0.333,0.667";
    assertThat(VariantToBqUtils.convertToDefinedType(floatListStr, VCFHeaderLineType.Float))
            .isEqualTo(Arrays.asList(0.333, 0.667));

    // test int values
    String integerStr = "23";
    assertThat(VariantToBqUtils.convertToDefinedType(integerStr, VCFHeaderLineType.Integer))
            .isEqualTo(23);

    // test float values
    String floatStr = "0.333";
    assertThat(VariantToBqUtils.convertToDefinedType(floatStr, VCFHeaderLineType.Float))
            .isEqualTo(0.333);

    // test String values
    String str = "T";
    assertThat(VariantToBqUtils.convertToDefinedType(str, VCFHeaderLineType.String))
            .isEqualTo("T");
  }
}
