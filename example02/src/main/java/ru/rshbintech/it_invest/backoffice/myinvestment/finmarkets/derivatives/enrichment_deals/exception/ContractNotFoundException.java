package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception;

public class ContractNotFoundException extends RuntimeException {
    public ContractNotFoundException() {
        super("Contract not found.");
    }
}
