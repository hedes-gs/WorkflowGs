package com.gs.photo.common.workflow;

import java.util.function.Consumer;

import org.slf4j.Logger;

public class LoggerWrapper {
    final org.slf4j.Logger logger;

    public void debug(Consumer<org.slf4j.Logger> c) {
        if (this.logger.isDebugEnabled()) {
            c.accept(this.logger);
        }
    }

    public void info(Consumer<org.slf4j.Logger> c) {
        if (this.logger.isInfoEnabled()) {
            c.accept(this.logger);
        }
    }

    public LoggerWrapper(Logger logger) {
        super();
        this.logger = logger;
    }

}
