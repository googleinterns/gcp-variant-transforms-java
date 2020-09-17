// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.common.base.Joiner;
import org.apache.beam.sdk.io.FileIO;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 * CSV sink class, includes header of the CSV file and methods of opening and writing CSV records.
 */
public class MalformedRecordCSVSink implements FileIO.Sink<List<String>>{
  private String header;
  private PrintWriter writer;

  public MalformedRecordCSVSink(List<String> colNames) {
    this.header = Joiner.on(",").join(colNames);
  }

  public void open(WritableByteChannel channel) throws IOException {
    writer = new PrintWriter(Channels.newOutputStream(channel));
    writer.println(header);
  }

  public void write(List<String> element) throws IOException {
    writer.println(Joiner.on(",").join(element));
  }

  public void flush() throws IOException {
    writer.flush();
  }
}
