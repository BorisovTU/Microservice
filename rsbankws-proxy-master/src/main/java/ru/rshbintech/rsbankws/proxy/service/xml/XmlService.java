package ru.rshbintech.rsbankws.proxy.service.xml;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import ru.rshbintech.rsbankws.proxy.model.XmlInfo;
import ru.rshbintech.rsbankws.proxy.model.enums.XmlType;
import ru.rshbintech.rsbankws.proxy.model.exception.IncorrectInputXmlDataException;
import ru.rshbintech.rsbankws.proxy.model.exception.XmlProcessingException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.io.StringWriter;

import static javax.xml.XMLConstants.ACCESS_EXTERNAL_DTD;
import static javax.xml.XMLConstants.ACCESS_EXTERNAL_SCHEMA;
import static javax.xml.XMLConstants.ACCESS_EXTERNAL_STYLESHEET;
import static javax.xml.xpath.XPathConstants.NODE;
import static javax.xml.xpath.XPathConstants.NODESET;
import static javax.xml.xpath.XPathConstants.STRING;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.MSG_ERROR_TAG_OR_VALUE_NOT_EXISTS;

@Service
@RequiredArgsConstructor
public class XmlService {

    @NonNull
    public XmlInfo parseXml(@NonNull String xmlAsString, @NonNull XmlType xmlType) {
        if (StringUtils.isBlank(xmlAsString)) {
            throw new XmlProcessingException(xmlAsString, xmlType, "XML не может быть пуст");
        }
        final String normalizedXmlAsString = StringUtils.trim(xmlAsString);
        try (StringReader xmlStringReader = new StringReader(normalizedXmlAsString)) {
            final Document parsedXml = createDocumentBuilder().parse(new InputSource(xmlStringReader));
            if (parsedXml == null) {
                throw new XmlProcessingException(
                        normalizedXmlAsString,
                        xmlType,
                        "Результат разбора XML не может быть пуст"
                );
            }
            return XmlInfo.builder()
                    .xmlAsString(normalizedXmlAsString)
                    .parsedXml(parsedXml)
                    .xmlType(xmlType)
                    .build();
        } catch (Exception e) {
            throw new XmlProcessingException(normalizedXmlAsString, xmlType, "Ошибка разбора XML", e);
        }
    }

    @NonNull
    public String getAttributeValueAsString(@NonNull XmlInfo xmlInfo, @NonNull String pathToAttribute) {
        try {
            final XPath xPath = XPathFactory.newInstance().newXPath();
            final XPathExpression xPathExpression = xPath.compile(pathToAttribute);
            return ((NodeList) xPathExpression.evaluate(xmlInfo.getParsedXml(), NODESET)).item(0).getNodeValue();
        } catch (Exception e) {
            throw new XmlProcessingException(
                    xmlInfo,
                    String.format(
                            "Ошибка получения значения аттрибута по пути = [%s]",
                            normalizePath(pathToAttribute)
                    ),
                    e
            );
        }
    }

    @NonNull
    public boolean isTagNotExists(@NonNull XmlInfo xmlInfo, @NonNull String pathToNode) {
        return getNode(xmlInfo, pathToNode) == null;
    }

    public void checkTagExistsWithThrowError(@NonNull XmlInfo xmlInfo, @NonNull String pathToNode) {
        if (isTagNotExists(xmlInfo, pathToNode)) {
            throw new IncorrectInputXmlDataException(prepareMsgErrorTagOrValueNotExists(pathToNode));
        }
    }

    @NonNull
    public String getTagValueAsString(@NonNull XmlInfo xmlInfo, @NonNull String pathToNode) {
        try {
            final XPath xPath = XPathFactory.newInstance().newXPath();
            final XPathExpression xPathExpression = xPath.compile(pathToNode + "//text()");
            return (String) xPathExpression.evaluate(xmlInfo.getParsedXml(), STRING);
        } catch (Exception e) {
            throw new XmlProcessingException(
                    xmlInfo,
                    String.format("Ошибка получения значения тега по пути = [%s]", normalizePath(pathToNode)),
                    e
            );
        }
    }

    @NonNull
    public String getTagValueAsStringWithThrowErrorIfEmpty(@NonNull XmlInfo xmlInfo, @NonNull String pathToNode) {
        final String tagValue = getTagValueAsString(xmlInfo, pathToNode);
        if (StringUtils.isEmpty(tagValue)) {
            throw new IncorrectInputXmlDataException(prepareMsgErrorTagOrValueNotExists(pathToNode));
        }
        return tagValue;
    }

    @NonNull
    public String getTagValueAsStringWithExistsAndEmptyCheck(@NonNull XmlInfo xmlInfo, @NonNull String pathToNode) {
        checkTagExistsWithThrowError(xmlInfo, pathToNode);
        return getTagValueAsStringWithThrowErrorIfEmpty(xmlInfo, pathToNode);
    }

    @NonNull
    public String getOnlyOneChildTagNameForCurrentTag(@NonNull XmlInfo xmlInfo,
                                                      @NonNull String pathToNode) {
        try {
            final XPath xPath = XPathFactory.newInstance().newXPath();
            final XPathExpression xPathExpression = xPath.compile(pathToNode);
            final NodeList currentNodeList = (NodeList) xPathExpression.evaluate(xmlInfo.getParsedXml(), NODE);
            final Node childNode = getOnlyOneChildNode(currentNodeList);
            if (childNode == null) {
                throw new XmlProcessingException(
                        xmlInfo,
                        String.format(
                                "Должен присутствовать только один дочерний тег для тега по пути = [%s]",
                                normalizePath(pathToNode)
                        )
                );
            }
            return childNode.getNodeName();
        } catch (Exception e) {
            throw new XmlProcessingException(
                    xmlInfo,
                    String.format("Ошибка получения тега по пути = [%s]", normalizePath(pathToNode)),
                    e
            );
        }
    }

    @NonNull
    public XmlInfo getNestedXmlInfo(@NonNull XmlInfo baseXmlInfo,
                                    @NonNull XmlType nestedXmlType,
                                    @NonNull String pathToNestedXml) {
        return parseXml(getTagValueAsString(baseXmlInfo, pathToNestedXml), nestedXmlType);
    }

    @Nullable
    public void addNewTag(@NonNull XmlInfo xmlInfo,
                          @NonNull String pathToDestNode,
                          @NonNull String tagName,
                          @NonNull String tagValue) {
        final Node destNode = getNode(xmlInfo, pathToDestNode);
        if (destNode == null) {
            throw new XmlProcessingException(
                    xmlInfo,
                    String.format(
                            "Не удалось получить тег для записи по пути = [%s]",
                            normalizePath(pathToDestNode)
                    )
            );
        }
        try {
            Element newTag = xmlInfo.getParsedXml().createElement(tagName);
            newTag.setTextContent(tagValue);
            destNode.appendChild(newTag);
        } catch (Exception e) {
            throw new XmlProcessingException(
                    xmlInfo,
                    String.format(
                            "Ошибка добавления тега с наименованием = [%s] и значением = [%s] в тег по пути = [%s]",
                            tagName,
                            tagValue,
                            normalizePath(pathToDestNode)
                    ),
                    e
            );
        }
    }

    @NonNull
    public String writeXmlAsString(@NonNull XmlInfo xmlInfo) {
        try (StringWriter stringWriter = new StringWriter()) {
            final Transformer transformer = createTransformer();
            final DOMSource domSource = new DOMSource(xmlInfo.getParsedXml());
            final StreamResult streamResult = new StreamResult(stringWriter);
            transformer.transform(domSource, streamResult);
            return stringWriter.toString();
        } catch (Exception e) {
            throw new XmlProcessingException(xmlInfo, "Ошибка записи XML в строку", e);
        }
    }

    @NonNull
    public String wrapReqIdAttrIfNeed(@NonNull String reqId) {
        /*
        Было решено с аналитиком на данный момент брать значение reqId из входящей XML methodCall.
        Если значение во входящей XML пусто, то и в исходящей XML его нужно оставить пустым.
         */
        return StringUtils.isEmpty(reqId) ? EMPTY : "{" + reqId + "}";
    }

    @Nullable
    private Node getNode(@NonNull XmlInfo xmlInfo, @NonNull String pathToNode) {
        try {
            final XPath xPath = XPathFactory.newInstance().newXPath();
            final XPathExpression xPathExpression = xPath.compile(pathToNode);
            return (Node) xPathExpression.evaluate(xmlInfo.getParsedXml(), NODE);
        } catch (Exception e) {
            throw new XmlProcessingException(
                    xmlInfo,
                    String.format("Ошибка получения тега по пути = [%s]", normalizePath(pathToNode)),
                    e
            );
        }
    }

    @NonNull
    private String prepareMsgErrorTagOrValueNotExists(@NonNull String pathToNode) {
        return String.format(MSG_ERROR_TAG_OR_VALUE_NOT_EXISTS, normalizePath(pathToNode));
    }

    @NonNull
    private String normalizePath(@NonNull String pathToNode) {
        return RegExUtils.replaceAll(StringUtils.substring(pathToNode, 2), "//", ".");
    }

    @Nullable
    private Node getOnlyOneChildNode(@NonNull NodeList nodeList) {
        Node node = null;
        boolean alreadyHasOnceElement = false;
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (nodeList.item(i) instanceof Element) {
                if (alreadyHasOnceElement) {
                    return null;
                } else {
                    node = nodeList.item(i);
                    alreadyHasOnceElement = true;
                }
            }
        }
        return node;
    }

    @NonNull
    private DocumentBuilder createDocumentBuilder() throws ParserConfigurationException {
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        documentBuilderFactory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        documentBuilderFactory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        documentBuilderFactory.setAttribute(ACCESS_EXTERNAL_DTD, EMPTY);
        documentBuilderFactory.setAttribute(ACCESS_EXTERNAL_SCHEMA, EMPTY);
        documentBuilderFactory.setExpandEntityReferences(false);
        return documentBuilderFactory.newDocumentBuilder();
    }

    @NonNull
    private Transformer createTransformer() throws TransformerConfigurationException {
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        transformerFactory.setAttribute(ACCESS_EXTERNAL_DTD, EMPTY);
        transformerFactory.setAttribute(ACCESS_EXTERNAL_STYLESHEET, EMPTY);
        return transformerFactory.newTransformer();
    }

}
