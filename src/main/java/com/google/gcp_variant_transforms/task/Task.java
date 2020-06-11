// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.task;

import java.io.IOException;

/** EntryPoint for all tasks. */
public interface Task {
  /**
   * Runs the application for required task.
   *
   * @throws IOException
   */
  public abstract void run() throws IOException;
}
