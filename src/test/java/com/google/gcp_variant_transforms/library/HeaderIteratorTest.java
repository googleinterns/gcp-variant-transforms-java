package com.google.gcp_variant_transforms.library;

import com.google.gcp_variant_transforms.library.HeaderIterator;
import static com.google.common.truth.Truth.*;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Units tests for HeaderIterator.java
 */
public class HeaderIteratorTest {

  @Test
  public void testHeaderLinesHasNext_whenCheckingBoolean_thenTrue() {
    // Arrange
    int listSize = 5;
    ImmutableList.Builder<String> headerLinesBuilder = new ImmutableList.Builder<String>();
    ImmutableList<String> headerLines;
    HeaderIterator headerIterator;
    for (int i = 0; i < listSize; i++) {
      headerLinesBuilder.add("##sampleHeaderLine=" + i);
    }
    headerLines = headerLinesBuilder.build();
    headerIterator = new HeaderIterator(headerLines);
    // Act

    // Assert
    assertThat(headerIterator.hasNext()).isTrue();

  }

  @Test
  public void testHeaderLinesHasNext_whenCheckingBoolean_thenFalse() {
    // Arrange
    int listSize = 5;
    ImmutableList.Builder<String> headerLinesBuilder = new ImmutableList.Builder<String>();
    ImmutableList<String> headerLines;
    HeaderIterator headerIterator;
    for (int i = 0; i < listSize; i++) {
      headerLinesBuilder.add("##sampleHeaderLine=" + i);
    }
    headerLines = headerLinesBuilder.build();
    headerIterator = new HeaderIterator(headerLines);
    // Act
    for (int i = 0; i < listSize; i++) {
      headerIterator.next();
    }
    // Assert
    assertThat(headerIterator.hasNext()).isFalse();
  }

  @Test
  public void testHeaderLinesHasNext_whenEmptyCheckingBoolean_thenFalse() {
    // Arrange
    int listSize = 5;
    ImmutableList.Builder<String> headerLinesBuilder = new ImmutableList.Builder<String>();
    ImmutableList<String> headerLines; 
    HeaderIterator headerIterator;
    headerLines = headerLinesBuilder.build(); // empty headerList
    headerIterator = new HeaderIterator(headerLines);
    // Act

    // Assert
    assertThat(headerIterator.hasNext()).isFalse();
  }

  @Test
  public void testHeaderLinesNext_whenCheckingString_thenContains() {
    // Arrange
    int listSize = 5;
    CharSequence expectedHeaderLine; 
    ImmutableList<String> headerLines;
    HeaderIterator headerIterator;
    ImmutableList.Builder<String> headerLinesBuilder = new ImmutableList.Builder<String>();
    for (int i = 0; i < listSize; i++) {
      headerLinesBuilder.add("##sampleHeaderLine=" + i);
    }
    headerLines = headerLinesBuilder.build();
    headerIterator = new HeaderIterator(headerLines);
    expectedHeaderLine = "##sampleHeaderLine=0";
    // Act

    // Assert
    assertThat(headerIterator.next()).contains(expectedHeaderLine);

  }

  @Test
  public void testHeaderLinesNext_whenCheckingString_thenException() {
    // Arrange
    int listSize = 5;
    HeaderIterator headerIterator;
    ImmutableList<String> headerLines;
    Exception actualException;
    Class expectedException = IndexOutOfBoundsException.class;
    ImmutableList.Builder<String> headerLinesBuilder = new ImmutableList.Builder<String>();
    headerLines = headerLinesBuilder.build(); // empty headerList
    headerIterator = new HeaderIterator(headerLines);
    // Act
    actualException = assertThrows(IndexOutOfBoundsException.class, () ->
        headerIterator.next());
    // Assert
    assertThat(actualException).isInstanceOf(expectedException);

  }
}