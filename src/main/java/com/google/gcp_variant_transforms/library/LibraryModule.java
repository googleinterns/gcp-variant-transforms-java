// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.inject.AbstractModule;

/** Binds Library level classes. */
public class LibraryModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(VcfParser.class).to(VcfParserImpl.class);
    bind(HeaderReader.class).to(DummyHeaderReader.class);
  }
}
