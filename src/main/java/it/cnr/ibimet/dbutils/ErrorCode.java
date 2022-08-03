package it.cnr.ibimet.dbutils;

public class ErrorCode {
    public final static String POLYGON_TOO_BIG_STR = "-2: PROVIDED POLYGON IS TOO BIG";
    public final static String DATA_NOT_FOUND_STR = "-1: DATA NOT FOUND";
    public final static String POLYGON_IS_MANDATORY_STR = "-3: YOU HAVE TO PROVIDE A POLYGON FOR DATA EXTRACTION";
    public final static String WRONG_IMAGE_TYPE_STR = "-4: WRONG IMAGE TYPE";

    public final static int POLYGON_TOO_BIG = -2;
    public final static int DATA_NOT_FOUND = -1;
    public final static int POLYGON_IS_MANDATORY = -3;
    public final static int WRONG_IMAGE_TYPE = -4;

    private int errCode;

    public ErrorCode(int errCode) {
        this.errCode = errCode;
    }



    public int getErrCode() {
        return errCode;
    }

    public void setErrCode(int errCode) {
        this.errCode = errCode;
    }
}
