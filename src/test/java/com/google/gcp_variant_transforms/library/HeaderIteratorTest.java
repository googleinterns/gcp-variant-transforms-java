package com.google.gcp_variant_transforms.library;

import com.google.gcp_variant_transforms.library.HeaderIterator;
import static com.google.common.truth.Truth.*;
import static com.google.common.truth.Truth8.*;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.*;
import sun.jvm.hotspot.memory.HeapBlock;

/**
 * Units tests for HeaderIterator.java
 */
public class HeaderIteratorTest {

  pubilc void testHeaderLinesHasNext_whenCheckingBoolean_thenTrue() {
    //arrange
    int listSize = 5;
    ImmutableList.Builder<String> headerLinesBuilder = new ImmutableList.Builder<String>();
    ImmutableList<String> headerLines;
    for (int i = 0; i < listList; i++) {
      headerLinesBuilder.add("##sampleHeaderLine=" + i);
    }
    headerLines = headerLinesBuilder.build();
    HeaderIterator headerIterator = new HeaderIterator(headerLines);
    //act

    //assert
    assertThat(headerIterator.hasNext()).isTrue();

  }
}