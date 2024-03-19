package com.gs.photo.workflow.extimginfo.impl;

import java.io.IOException;

public class InvalidHeaderException extends IOException {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public InvalidHeaderException() { super(); }

    public InvalidHeaderException(
        String message,
        Throwable cause
    ) { super(message,
        cause); }

    public InvalidHeaderException(String message) { super(message); }

    public InvalidHeaderException(Throwable cause) { super(cause); }

}
