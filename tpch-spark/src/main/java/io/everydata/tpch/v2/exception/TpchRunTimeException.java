package io.everydata.tpch.v2.exception;

public class TpchRunTimeException
        extends RuntimeException
{
    public TpchRunTimeException(String msg)
    {
        super(msg);
    }
}
