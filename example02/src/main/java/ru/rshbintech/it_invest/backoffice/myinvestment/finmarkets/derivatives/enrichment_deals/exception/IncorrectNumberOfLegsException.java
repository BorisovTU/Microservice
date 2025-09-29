package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception;

public class IncorrectNumberOfLegsException extends RuntimeException {
    public IncorrectNumberOfLegsException() {
        super("Incorrect number of legs");
    }
}
