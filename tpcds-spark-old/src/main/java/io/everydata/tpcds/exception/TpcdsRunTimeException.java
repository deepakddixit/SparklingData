package io.everydata.tpcds.exception;

public class TpcdsRunTimeException
    extends RuntimeException {
  public TpcdsRunTimeException(String msg) {
    super(msg);
  }
}
