package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception;

public class DrawingDateNotDeterminedException extends RuntimeException {
    public DrawingDateNotDeterminedException() {
        super("Drawing date not determined");
    }
}
