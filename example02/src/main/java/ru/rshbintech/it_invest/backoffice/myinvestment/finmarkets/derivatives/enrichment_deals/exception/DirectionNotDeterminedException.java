package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception;

public class DirectionNotDeterminedException extends RuntimeException {
    public DirectionNotDeterminedException() {
        super("Direction not determined");
    }
}
