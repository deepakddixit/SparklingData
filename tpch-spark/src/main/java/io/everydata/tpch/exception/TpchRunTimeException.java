package io.everydata.tpch.exception;

public class TpchRunTimeException
    extends RuntimeException {
  public TpchRunTimeException(String msg) {
    super(msg);
  }
}
