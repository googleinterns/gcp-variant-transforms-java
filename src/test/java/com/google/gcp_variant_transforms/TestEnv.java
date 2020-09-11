// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms;

import com.google.gcp_variant_transforms.library.LibraryTestModule;
import com.google.guiceberry.GuiceBerryModule;
import com.google.inject.AbstractModule;

public class TestEnv extends AbstractModule {

  @Override
  protected void configure() {
    install(new GuiceBerryModule());
    install(new LibraryTestModule());
  }
}
