// Copyright 2020 Google LLC
 
package com.google.gcp_variant_transforms.options;
 
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
 
import com.google.common.collect.ImmutableList;
import com.google.gcp_variant_transforms.options.VcfToBqContext;
import java.io.IOException;
import org.junit.Test;
 
/**
* Units tests for VcfToBqContext.java
*/
public class VcfToBqContextTest {
 
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
   String inputFile = "sampleInputFile.txt";
   String output = "sampleOutputFile.txt";
   VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(inputFile, output);
   VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);
 
   assertThat(vcfToBqContext.getInputFile()).matches(inputFile);
   assertThat(vcfToBqContext.getOutput()).matches(output);
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
   String inputFile = "sampleInputFile.txt";
   VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(
       inputFile,
       "sampleOutputFile.txt");
   VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);
 
   assertThat(vcfToBqContext.getInputFile()).matches(inputFile);
 }
 
 @Test
 public void testVcfContext_whenInputFileNull_thenNull() throws IOException {
   VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(
       null,
       "sampleOutputFile.txt");
   VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);
  
   assertThat(vcfToBqContext.getInputFile()).isNull();
 
 }
 
 @Test
 public void testVcfContext_whenGetOutput_thenMatches() throws IOException {
   String output = "sampleOutputFile.txt";
   VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(
       "sampleInputFile.txt",
       output);
   VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);   
 
   assertThat(vcfToBqContext.getOutput()).matches(output);
 }
 
 @Test
 public void testVcfContext_whenNullOutput_thenException() throws IOException {
   VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(
       "sampleInputFile.txt",
       null);
 
   assertThrows(IOException.class, () ->
       new VcfToBqContext(mockedVcfToBqOptions));   
 }
 
 @Test
 public void testVcfContext_whenSetHeaderLines_thenMatches() throws IOException {
   VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(
       "sampleInputFile.txt",
       "sampleOutputFile.txt");
   VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);   
   ImmutableList<String> headerLines = createHeaderLines();
   vcfToBqContext.setHeaderLines(headerLines);
 
   assertThat(vcfToBqContext.getHeaderLines()).containsExactlyElementsIn(headerLines);
 }
 
 @Test
 public void testVcfContext_whenSetHeaderLines_thenNull() throws IOException {
   VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(
       "sampleInputFile.txt",
       "sampleOutputFile.txt");
   VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);   
   vcfToBqContext.setHeaderLines(null);
 
   assertThat(vcfToBqContext.getHeaderLines()).isNull();
 }
 
 @Test
 public void testVcfContext_whenGetHeaderLines_thenNull() throws IOException {
   VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(
       "sampleInputFile.txt",
       "sampleOutputFile.txt");
   VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);   
 
   assertThat(vcfToBqContext.getHeaderLines()).isNull();
 }
}
