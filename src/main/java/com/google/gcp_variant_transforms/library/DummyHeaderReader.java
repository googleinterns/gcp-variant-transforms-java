// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.common.collect.ImmutableList;
import com.google.gcp_variant_transforms.options.VcfToBqContext;
import com.google.inject.Inject;
import org.apache.beam.sdk.io.FileSystems;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * Dummy implementation of {@link HeaderReader}, which sequentially reads headers from single
 * a file. Used for Demo.
 */
public class DummyHeaderReader implements HeaderReader {

  private final VcfToBqContext context;

  @Inject
  public DummyHeaderReader(VcfToBqContext context) {
    this.context = context;
  }

  @Override
  public ImmutableList<String> getHeaderLines() throws IOException {
    ImmutableList.Builder<String> headers = new ImmutableList.Builder<String>();
    BufferedReader bufferedReader = null;

    ReadableByteChannel readableByteChannel = FileSystems.open(
        FileSystems.matchNewResource(context.getInputFile(), false));
    InputStream stream = Channels.newInputStream(readableByteChannel);
    bufferedReader = new BufferedReader(new InputStreamReader(stream));

    String line = null;

    while ((line = bufferedReader.readLine()) != null) {
      if (!line.startsWith("#")) {
        break;
      }
      headers.add(line);
    }
    bufferedReader.close();

    return headers.build();
  }
}
