package com.iblync.kafka.connect.bigquery.sink;

import static java.util.Objects.requireNonNull;

import org.reactivestreams.Publisher; // ✅ This is the correct Publisher
import reactor.core.publisher.Mono;

public class SinkAction {
    private static final SinkAction IGNORE = new SinkAction(Mono.empty(), ConcurrencyHint.alwaysConcurrent());

    // Mono --> waiting for a future result.
    private final Mono<Void> action;
    private final ConcurrencyHint concurrencyHint;

    public static SinkAction ignore() {
        return IGNORE;
    }

    public SinkAction(Publisher<?> publisher, ConcurrencyHint concurrencyHint) {
        this.action = Mono.from(publisher).then(); // converts any Publisher<?> to Mono<Void>
        this.concurrencyHint = requireNonNull(concurrencyHint);
    }

    public Mono<Void> action() {
        return action;
    }

    public ConcurrencyHint concurrencyHint() {
        return concurrencyHint;
    }
}

