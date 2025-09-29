package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception;

public class MarketSchemeNotFound extends RuntimeException {
    public MarketSchemeNotFound() {
        super("Market scheme not found");
    }
}
