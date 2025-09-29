package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception;

public class FinInstrNotFoundException extends RuntimeException {
    public FinInstrNotFoundException() {
        super("Financial instrument not found.");
    }
}
