package com.iblync.kafka.connect.bigquery.util;

import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.iblync.kafka.connect.bigquery.sink.ConcurrencyHint;

public class BatchBuilder<T> {
  private final List<List<T>> batches = new ArrayList<>();
  private final Set<ConcurrencyHint> hintsInCurrentBatch = new HashSet<>();
  private List<T> currentBatch = null;

  /**
   * Inspects the concurrency hint to see if a new batch must be started,
   * then adds the item to the current batch.
   *
   * @param item item to add to a batch
   * @param hint determines batch boundaries
   * @return this builder
   */
  public BatchBuilder<T> add(T item, ConcurrencyHint hint) {
    if (hint == ConcurrencyHint.neverConcurrent()) {
      // Special case so we don't waste tons of space
      // if all of the items are "never concurrent"
      doAddSingleton(item);
      return this;
    }

    if (hint != ConcurrencyHint.alwaysConcurrent() && !hintsInCurrentBatch.add(hint)) {
      insertBarrier();
      hintsInCurrentBatch.add(hint);
    }

    doAdd(item);
    return this;
  }

  /**
   * Returns the list of batches of items from previous calls to {@link #add(Object, ConcurrencyHint)}.
   */
  public List<List<T>> build() {
    return batches;
  }

  private void insertBarrier() {
    currentBatch = null;
    hintsInCurrentBatch.clear();
  }

  private void doAdd(T item) {
    if (currentBatch == null) {
      currentBatch = new ArrayList<>();
      batches.add(currentBatch);
    }
    currentBatch.add(item);
  }

  private void doAddSingleton(T item) {
    batches.add(singletonList(item));
    insertBarrier();
  }

  @Override
  public String toString() {
    return "BatchBuilder{" +
        "hintsInCurrentBatch=" + hintsInCurrentBatch +
        ", batches=" + batches +
        ", currentBatch=" + currentBatch +
        '}';
  }
}

