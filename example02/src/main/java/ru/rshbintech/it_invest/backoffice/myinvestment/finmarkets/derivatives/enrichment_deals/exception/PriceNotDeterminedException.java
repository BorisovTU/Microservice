package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception;

public class PriceNotDeterminedException extends RuntimeException {
    public PriceNotDeterminedException() {
        super("Price not determined");
    }
}
