package com.kaua.event.driven;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class StubAggregateLifecycleExtension extends StubAggregateLifecycle implements AfterEachCallback, BeforeEachCallback {

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        activate();
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        close();
    }
}
