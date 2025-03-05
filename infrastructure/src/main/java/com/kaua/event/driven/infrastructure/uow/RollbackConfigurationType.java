package com.kaua.event.driven.infrastructure.uow;

public enum RollbackConfigurationType implements RollbackConfiguration {

    NEVER {
        @Override
        public boolean rollBackOn(Throwable throwable) {
            return false;
        }
    },

    ANY_THROWABLE {
        @Override
        public boolean rollBackOn(Throwable throwable) {
            return true;
        }
    },

    UNCHECKED_EXCEPTIONS {
        @Override
        public boolean rollBackOn(Throwable throwable) {
            return !(throwable instanceof Exception) || throwable instanceof RuntimeException;
        }
    },

    RUNTIME_EXCEPTIONS {
        @Override
        public boolean rollBackOn(Throwable throwable) {
            return throwable instanceof RuntimeException;
        }
    }
}
