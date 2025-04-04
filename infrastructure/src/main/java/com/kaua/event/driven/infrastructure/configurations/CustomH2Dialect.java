package com.kaua.event.driven.infrastructure.configurations;

import org.hibernate.LockOptions;
import org.hibernate.dialect.H2Dialect;

/*
*
* Custom H2 Dialect to support SKIP LOCKED
* This is used in tests, not in production
* */
public class CustomH2Dialect extends H2Dialect {

    @Override
    public boolean supportsSkipLocked() {
        return true;
    }

    @Override
    public String getForUpdateNowaitString() {
        return "";
    }

    @Override
    public String getForUpdateSkipLockedString() {
        return "";
    }

    @Override
    public String getForUpdateString(LockOptions lockOptions) {
        return "";
    }
}
