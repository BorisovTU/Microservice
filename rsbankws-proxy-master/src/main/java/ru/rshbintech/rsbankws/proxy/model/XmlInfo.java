package ru.rshbintech.rsbankws.proxy.model;

import lombok.Builder;
import lombok.Getter;
import org.w3c.dom.Document;
import ru.rshbintech.rsbankws.proxy.model.enums.XmlType;

@Getter
@Builder
public class XmlInfo {

    private String xmlAsString;
    private Document parsedXml;
    private XmlType xmlType;

}
