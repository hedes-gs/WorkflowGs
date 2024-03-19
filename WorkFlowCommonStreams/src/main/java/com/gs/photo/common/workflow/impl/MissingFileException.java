package com.gs.photo.common.workflow.impl;

import com.workflow.model.files.FileToProcess;

public class MissingFileException extends Exception {

    protected FileToProcess   fileToProcess;

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public MissingFileException(FileToProcess fileToProcess) {
        super();
        this.fileToProcess = fileToProcess;
    }

    public MissingFileException() { super(); }

    public MissingFileException(
        String message,
        Throwable cause,
        boolean enableSuppression,
        boolean writableStackTrace
    ) {
        super(message,
            cause,
            enableSuppression,
            writableStackTrace);
        // TODO Auto-generated constructor stub
    }

    public MissingFileException(
        String message,
        Throwable cause
    ) { super(message,
        cause); }

    public MissingFileException(String message) { super(message); }

    public MissingFileException(Throwable cause) { super(cause); }

}
