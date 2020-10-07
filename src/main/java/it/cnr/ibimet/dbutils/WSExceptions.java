package it.cnr.ibimet.dbutils;

import com.sun.javaws.exceptions.ErrorCodeResponseException;

public class WSExceptions extends Exception {
    private static final long serialVersionUID = 7718828512143293558L;

    private final ErrorCode code;

    public WSExceptions(ErrorCode code) {
        super();
        this.code = code;
    }



    public WSExceptions(String message, Throwable cause, ErrorCode code) {
        super(message, cause);
        this.code = code;
    }

    public WSExceptions(String message, ErrorCode code) {
        super(message);
        this.code = code;
    }

    public WSExceptions(Throwable cause, ErrorCode code) {
        super(cause);
        this.code = code;
    }

    public ErrorCode getCode() {
        return this.code;
    }
}
