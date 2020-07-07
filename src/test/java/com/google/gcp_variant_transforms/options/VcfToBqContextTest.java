// Copyright 2020 Google LLC
 
package com.google.gcp_variant_transforms.options;
 
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
 
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.junit.Test;
 
/**
* Units tests for VcfToBqContext.java
*/
public class VcfToBqContextTest {
  
  private static final String INPUT_FILE = "sampleInputFile.txt";
  private static final String OUTPUT = "sampleOutput";
  private final VcfToBqOptions MOCKED_VCFTOBQ_OPTIONS = create_mockedVcfToBqOptions(INPUT_FILE, OUTPUT);
 
  public VcfToBqOptions create_mockedVcfToBqOptions(String inputFile, String output) {
    VcfToBqOptions mockedVcfToBqOptions = mock(VcfToBqOptions.class);
    when(mockedVcfToBqOptions.getInputFile()).thenReturn(inputFile);
    when(mockedVcfToBqOptions.getOutput()).thenReturn(output);
    return mockedVcfToBqOptions;
  }
 
  public ImmutableList<String> createHeaderLines() {
    ImmutableList.Builder<String> headerLinesBuilder = new ImmutableList.Builder<String>();
    for (int i = 0; i < 10; i++) {
      headerLinesBuilder.add("##sampleHeaderLine=" + i);
    }
    return headerLinesBuilder.build();
  }
 
  @Test
  public void testVcfContextConstructor_whenCompareFields_thenMatches() throws IOException {
    VcfToBqContext vcfToBqContext = new VcfToBqContext(MOCKED_VCFTOBQ_OPTIONS);
 
    assertThat(vcfToBqContext.getInputFile()).matches(INPUT_FILE);
    assertThat(vcfToBqContext.getOutput()).matches(OUTPUT);
    assertThat(vcfToBqContext.getHeaderLines()).isNull();
 }
 
  @Test
  public void testVcfContext_whenValidateFlags_thenException(){
    VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(null, null);
   
    assertThrows(IOException.class, () ->
        new VcfToBqContext(mockedVcfToBqOptions)); 
  }
 
  @Test
  public void testVcfContext_whenGetInputFile_thenMatches() throws IOException {
    VcfToBqContext vcfToBqContext = new VcfToBqContext(MOCKED_VCFTOBQ_OPTIONS);
 
    assertThat(vcfToBqContext.getInputFile()).matches(INPUT_FILE);
  }
 
  @Test
  public void testVcfContext_whenInputFileNull_thenNull() throws IOException {
    VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(
        null,
        OUTPUT);
    VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);
  
    assertThat(vcfToBqContext.getInputFile()).isNull();
  }
 
  @Test
  public void testVcfContext_whenGetOutput_thenMatches() throws IOException {
    VcfToBqContext vcfToBqContext = new VcfToBqContext(MOCKED_VCFTOBQ_OPTIONS);   
 
    assertThat(vcfToBqContext.getOutput()).matches(OUTPUT);
  }
 
  @Test
  public void testVcfContext_whenNullOutput_thenException() throws IOException {
    VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(
        INPUT_FILE,
        null);
 
    assertThrows(IOException.class, () ->
        new VcfToBqContext(mockedVcfToBqOptions));   
  }
 
 @Test
 public void testVcfContext_whenSetHeaderLines_thenMatches() throws IOException {
    VcfToBqContext vcfToBqContext = new VcfToBqContext(MOCKED_VCFTOBQ_OPTIONS);   
    ImmutableList<String> headerLines = createHeaderLines();
    vcfToBqContext.setHeaderLines(headerLines);
 
    assertThat(vcfToBqContext.getHeaderLines()).containsExactlyElementsIn(headerLines);
  }
 
  @Test
  public void testVcfContext_whenSetHeaderLines_thenNull() throws IOException {
    VcfToBqContext vcfToBqContext = new VcfToBqContext(MOCKED_VCFTOBQ_OPTIONS);   
    vcfToBqContext.setHeaderLines(null);
 
    assertThat(vcfToBqContext.getHeaderLines()).isNull();
  }
 
  @Test
  public void testVcfContext_whenGetHeaderLines_thenNull() throws IOException {
    VcfToBqContext vcfToBqContext = new VcfToBqContext(MOCKED_VCFTOBQ_OPTIONS);   
 
    assertThat(vcfToBqContext.getHeaderLines()).isNull();
  }
}
