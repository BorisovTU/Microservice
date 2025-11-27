<?xml version="1.0" encoding="UTF-8"?>
<!-- Преобразование интеграционного xml-сообщения XML RPC в IFX -->
<!-- Автор: Карпов В.А. -->

<xsl:stylesheet
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:n0="http://www.softlab.ru/xml-rpc/schema"
    xmlns:xs = "http://www.w3.org/2001/XMLSchema"
    xmlns="http://www.softlab.ru/xml-rpc/schema"
    version="1.0">
    
    <!-- Импорт шаблонов преобразований -->
    <!--
    <xsl:import href = "t_secur_xml_status_to_ifx.xsl"/>
    -->

    <!-- Элементы Text будут выводиться в секции CDATA -->
    <!-- По какой-то причине стандартная декларация заполнялась UTF-16 вместо UTF-8 -->
    <xsl:output method = "xml" indent = "yes" encoding="UTF-8" omit-xml-declaration = "yes" cdata-section-elements = "Text"/>
    <xsl:strip-space elements = "*"/>

    <!-- Глобальная переменная с наименованием сервиса -->
    <xsl:variable name = "serv_name" select = "string(/n0:methodResponse/n0:params/n0:param/n0:value/n0:struct/n0:member/n0:name)"/>
    
    <!-- Глобальная переменная с описанием объекта сервиса -->
    <xsl:variable name = "root_element" select = "document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:element[@name = $serv_name]"/>

    <!-- Обработка начинается с применения этого шаблона -->
    <xsl:template match = "/">
        <xsl:apply-templates select = "n0:methodResponse"/>
    </xsl:template>

    <!-- Шаблон формирования обязательных выражений и выбора обработки по типу ответа (ответ/ошибка) -->
    <xsl:template match = "n0:methodResponse">
        <!--
        <xsl:param name = "RespType" select = "local-name(./child::node()[1])"/>
        -->
        <!-- По какой-то причине стандартная декларация заполнялась UTF-16 вместо UTF-8 -->
        <xsl:text disable-output-escaping = "yes"><![CDATA[<?xml version="1.0" encoding="UTF-8"?>]]></xsl:text>

        <!--
        <xsl:choose>
            <xsl:when test="$RespType = 'fault'">
                <xsl:call-template name = "error_tmpl">
                    <xsl:with-param name = "is_error">1</xsl:with-param>
                </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
        -->
                <!-- Переход к преобразованию структур выходных данных -->
                <xsl:apply-templates select = "n0:params/n0:param/n0:value/n0:struct/n0:member/n0:value">
                    <xsl:with-param name = "xsd" select = "$root_element"/>
                </xsl:apply-templates>
        <!--
            </xsl:otherwise>
        </xsl:choose>
        -->
    </xsl:template>
    
    <!-- Шаблон формирования тэгов выходных данных -->
    <xsl:template match = "n0:params/n0:param/n0:value/n0:struct/n0:member/n0:value">
        <xsl:param name = "xsd"/>
        <xsl:apply-templates>
            <xsl:with-param name = "xsd" select = "$xsd"/>
            <xsl:with-param name = "elName" select = "../n0:name"/>
            <xsl:with-param name = "isRootElement" select = 'true()'/>
        </xsl:apply-templates>
    </xsl:template>
    
    <!-- Шаблон преобразования массива -->
    <xsl:template match = "n0:array">
        <xsl:param name = "xsd"/>
        <xsl:param name = "elName" select = "'array'"/>
        <xsl:param name = "el_ns" select = "$xsd/xs:annotation/xs:appinfo/text()"/>
        <xsl:variable name = "max_occurs" select = "$xsd/child::node()/xs:element/@maxOccurs"/>
        <xsl:element name = "{$elName}" namespace = "{$el_ns}">
            <xsl:choose>
                <xsl:when test = "$xsd/child::node()/xs:element[@name = $elName]/xs:complexType/child::node()/xs:element">
                    <xsl:choose>
                        <xsl:when test = "not($xsd/child::node()/xs:element[@name = $elName]/xs:complexType/xs:annotation/xs:appinfo)">
                            <xsl:apply-templates select = "n0:data/n0:value">
                                <xsl:with-param name = "xsd" select = "$xsd/child::node()/xs:element[@name = $elName]/xs:complexType"/> <!-- $xsd -->
                                <xsl:with-param name = "elName" select = "$xsd/child::node()/xs:element[@name = $elName]/xs:complexType/child::node()/xs:element/@name"/>
                                <xsl:with-param name = "el_ns" select = "$el_ns"/>
                            </xsl:apply-templates>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:apply-templates select = "n0:data/n0:value">
                                <xsl:with-param name = "xsd" select = "$xsd/child::node()/xs:element[@name = $elName]/xs:complexType"/> <!-- $xsd -->
                                <xsl:with-param name = "elName" select = "$xsd/child::node()/xs:element[@name = $elName]/xs:complexType/child::node()/xs:element/@name"/>
                            </xsl:apply-templates>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <xsl:when test = "$max_occurs = 'unbounded' or $max_occurs > 1">
                    <xsl:choose>
                        <xsl:when test = "not($xsd/xs:annotation/xs:appinfo)">
                            <xsl:apply-templates select = "n0:data/n0:value">
                                <xsl:with-param name = "xsd" select = "$xsd"/>
                                <xsl:with-param name = "elName" select = "$xsd/child::node()/xs:element/@name"/>
                                <xsl:with-param name = "el_ns" select = "$el_ns"/>
                            </xsl:apply-templates>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:apply-templates select = "n0:data/n0:value">
                                <xsl:with-param name = "xsd" select = "$xsd"/>
                                <xsl:with-param name = "elName" select = "$xsd/child::node()/xs:element/@name"/>
                            </xsl:apply-templates>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <xsl:when test = "$xsd/child::node()/xs:element[@name = $elName][@type]">
                    <xsl:choose>
                        <xsl:when test = "not(document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:complexType[@name = $xsd/child::node()/xs:element[@name = $elName]/@type]/xs:annotation/xs:appinfo)">
                            <xsl:apply-templates select = "n0:data/n0:value">
                                <xsl:with-param name = "xsd" select = "document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:complexType[@name = $xsd/child::node()/xs:element[@name = $elName]/@type]"/>
                                <xsl:with-param name = "elName" select = "document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:complexType[@name = $xsd/child::node()/xs:element[@name = $elName]/@type]/child::node()/xs:element/@name"/>
                                <xsl:with-param name = "el_ns" select = "$el_ns"/>
                            </xsl:apply-templates>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:apply-templates select = "n0:data/n0:value">
                                <xsl:with-param name = "xsd" select = "document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:complexType[@name = $xsd/child::node()/xs:element[@name = $elName]/@type]"/>
                                <xsl:with-param name = "elName" select = "document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:complexType[@name = $xsd/child::node()/xs:element[@name = $elName]/@type]/child::node()/xs:element/@name"/>
                            </xsl:apply-templates>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:choose>
                        <xsl:when test = "not(document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:complexType[@name = $xsd/child::node()/xs:element[@name = $elName]/@type]/xs:annotation/xs:appinfo)">
                            <xsl:apply-templates select = "n0:data/n0:value">
                                <xsl:with-param name = "xsd" select = "document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:complexType[@name = $xsd/child::node()/xs:element[@name = $elName]/@type]"/>
                                <xsl:with-param name = "elName" select = "document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:complexType[@name = $elName]/child::node()/xs:element/@name"/>
                                <xsl:with-param name = "el_ns" select = "$el_ns"/>
                            </xsl:apply-templates>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:apply-templates select = "n0:data/n0:value">
                                <xsl:with-param name = "xsd" select = "document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:complexType[@name = $xsd/child::node()/xs:element[@name = $elName]/@type]"/>
                                <xsl:with-param name = "elName" select = "document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:complexType[@name = $elName]/child::node()/xs:element/@name"/>
                            </xsl:apply-templates>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:element>
    </xsl:template>
    
    <!-- Шаблон преобразования элемента массива -->
    <xsl:template match = "n0:data/n0:value">
        <xsl:param name = "xsd"/>
        <xsl:param name = "elName" select = "'element'"/>
        <xsl:param name = "el_ns" select = "$xsd/xs:annotation/xs:appinfo/text()"/>
        <xsl:choose>
            <xsl:when test = "(./n0:struct) or (./n0:array)">
                <xsl:choose>
                    <xsl:when test = "not($xsd/xs:annotation/xs:appinfo)">
                        <xsl:apply-templates>
                            <xsl:with-param name = "xsd" select = "$xsd"/>
                            <xsl:with-param name = "elName" select = "$elName"/>
                            <xsl:with-param name = "el_ns" select = "$el_ns"/>
                        </xsl:apply-templates>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:apply-templates>
                            <xsl:with-param name = "xsd" select = "$xsd"/>
                            <xsl:with-param name = "elName" select = "$elName"/>
                        </xsl:apply-templates>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
            <xsl:otherwise>
                <xsl:element name = "{$elName}" namespace = "{$el_ns}">
                    <xsl:choose>
                        <xsl:when test = "local-name(./child::node()) = 'date'">
                            <xsl:value-of select = "substring(./child::node(),1,(string-length(./child::node()) - 1))"/>
                        </xsl:when>
                        <xsl:when test = "local-name(./child::node()) = 'timestamp'">
                            <xsl:value-of select = "substring(./child::node(),1,(string-length(./child::node()) - 1))"/>
                        </xsl:when>
                        <xsl:when test = "local-name(./child::node()) = 'money'">
                            <xsl:choose>
                                <xsl:when test = "contains(./child::node(), '.')">
                                    <xsl:value-of select = "concat(substring-before(./child::node(), '.'), '.', substring(substring-after(./child::node(), '.'), 1, 2))"/>
                                </xsl:when>
                                <xsl:when test = "contains(./child::node(), ',')">
                                    <xsl:value-of select = "concat(substring-before(./child::node(), ','), '.', substring(substring-after(./child::node(), ','), 1, 2))"/>
                                </xsl:when>
                                <xsl:otherwise>
                                    <xsl:value-of select = "./child::node()"/>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select = "./child::node()"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:element>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
    
    <!-- Шаблон преобразования структуры -->
    <xsl:template match = "n0:struct">
        <xsl:param name = "xsd"/>
        <xsl:param name = "elName" select = "'struct'"/>
        <xsl:param name = "el_ns" select = "$xsd/xs:annotation/xs:appinfo/text()"/>
        <xsl:param name = "isRootElement" select = 'false()'/>
        <!-- Структуру, все значения элементов которой равны NULL, не передаем -->
        <xsl:if test = "count(./n0:member) != count(./n0:member/n0:value/n0:NULL)">
            <xsl:element name = "{$elName}" namespace = "{$el_ns}">
                <xsl:choose>
                    <xsl:when test = "$isRootElement">
                        <xsl:choose>
                            <xsl:when test = "$xsd/child::node()/child::node()/xs:element[@name = $elName]/xs:complexType">
                                <xsl:choose>
                                    <xsl:when test = "not($xsd/child::node()/xs:element[@name = $elName]/xs:complexType/xs:annotation/xs:appinfo)">
                                        <xsl:apply-templates select = "n0:member">
                                            <xsl:with-param name = "xsd" select = "$xsd/child::node()/xs:element[@name = $elName]/xs:complexType"/>
                                            <xsl:with-param name = "el_ns" select = "$el_ns"/>
                                        </xsl:apply-templates>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <xsl:apply-templates select = "n0:member">
                                            <xsl:with-param name = "xsd" select = "$xsd/child::node()/xs:element[@name = $elName]/xs:complexType"/>
                                        </xsl:apply-templates>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:choose>
                                    <xsl:when test = "not(document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:complexType[@name = $xsd/@type]/xs:annotation/xs:appinfo)">
                                        <xsl:apply-templates select = "n0:member">
                                            <xsl:with-param name = "xsd" select = "document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:complexType[@name = $xsd/@type]"/>
                                            <xsl:with-param name = "el_ns" select = "$el_ns"/>
                                        </xsl:apply-templates>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <xsl:apply-templates select = "n0:member">
                                            <xsl:with-param name = "xsd" select = "document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:complexType[@name = $xsd/@type]"/>
                                        </xsl:apply-templates>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:choose>
                            <xsl:when test = "$xsd/child::node()/xs:element[@name = $elName]/xs:complexType">
                                <xsl:choose>
                                    <xsl:when test = "not($xsd/child::node()/xs:element[@name = $elName]/xs:complexType/xs:annotation/xs:appinfo)">
                                        <xsl:apply-templates select = "n0:member">
                                            <xsl:with-param name = "xsd" select = "$xsd/child::node()/xs:element[@name = $elName]/xs:complexType"/>
                                            <xsl:with-param name = "el_ns" select = "$el_ns"/>
                                        </xsl:apply-templates>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <xsl:apply-templates select = "n0:member">
                                            <xsl:with-param name = "xsd" select = "$xsd/child::node()/xs:element[@name = $elName]/xs:complexType"/>
                                        </xsl:apply-templates>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:choose>
                                    <xsl:when test = "not(document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:complexType[@name = $xsd/child::node()/xs:element[@name = $elName]/@type]/xs:annotation/xs:appinfo)">
                                        <xsl:apply-templates select = "n0:member">
                                            <xsl:with-param name = "xsd" select = "document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:complexType[@name = $xsd/child::node()/xs:element[@name = $elName]/@type]"/>
                                            <xsl:with-param name = "el_ns" select = "$el_ns"/>
                                        </xsl:apply-templates>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <xsl:apply-templates select = "n0:member">
                                            <xsl:with-param name = "xsd" select = "document('./s_out_rs_secur_xml_schema.xsd')/xs:schema/xs:complexType[@name = $xsd/child::node()/xs:element[@name = $elName]/@type]"/>
                                        </xsl:apply-templates>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:element>
        </xsl:if>
    </xsl:template>
    
    <!-- Шаблон преобразования элемента структуры -->
    <xsl:template match = "n0:member">
        <xsl:param name = "xsd"/>
        <xsl:param name = "elName" select = "./n0:name"/>
        <xsl:param name = "el_ns" select = "$xsd/xs:annotation/xs:appinfo/text()"/>
        <xsl:choose>
            <xsl:when test = "(./n0:value/n0:struct) or (./n0:value/n0:array)">
                <xsl:choose>
                    <xsl:when test = "not($xsd/xs:annotation/xs:appinfo)">
                        <xsl:apply-templates select = "./n0:value/child::node()">
                            <xsl:with-param name = "xsd" select = "$xsd"/>
                            <xsl:with-param name = "elName" select = "$elName"/>
                            <xsl:with-param name = "el_ns" select = "$el_ns"/>
                        </xsl:apply-templates>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:apply-templates select = "./n0:value/child::node()">
                            <xsl:with-param name = "xsd" select = "$xsd"/>
                            <xsl:with-param name = "elName" select = "$elName"/>
                        </xsl:apply-templates>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
            <xsl:otherwise>
                <xsl:choose>
                    <xsl:when test = "not(./n0:value/n0:NULL)">
                        <xsl:element name = "{$elName}" namespace = "{$el_ns}">
                            <xsl:choose>
                                <xsl:when test = "local-name(./n0:value/child::node()) = 'date'">
                                    <xsl:value-of select = "substring(./n0:value/child::node(),1,(string-length(./n0:value/child::node()) - 1))"/>
                                </xsl:when>
                                <xsl:when test = "local-name(./n0:value/child::node()) = 'timestamp'">
                                    <xsl:value-of select = "substring(./n0:value/child::node(),1,(string-length(./n0:value/child::node()) - 1))"/>
                                </xsl:when>
                                <xsl:when test = "local-name(./n0:value/child::node()) = 'money'">
                                    <xsl:choose>
                                        <xsl:when test = "contains(./n0:value/child::node(), '.')">
                                            <xsl:value-of select = "concat(substring-before(./n0:value/child::node(), '.'), '.', substring(substring-after(./n0:value/child::node(), '.'), 1, 2))"/>
                                        </xsl:when>
                                        <xsl:when test = "contains(./n0:value/child::node(), ',')">
                                            <xsl:value-of select = "concat(substring-before(./n0:value/child::node(), ','), '.', substring(substring-after(./n0:value/child::node(), ','), 1, 2))"/>
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <xsl:value-of select = "./n0:value/child::node()"/>
                                        </xsl:otherwise>
                                    </xsl:choose>
                                </xsl:when>
                                <xsl:otherwise>
                                    <xsl:value-of select = "./n0:value/child::node()"/>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:element>
                    </xsl:when>
                    <!-- Если значение NULL в строковом типе, то преобразуем в пустой тэг. Для остальных типов ничего не передаем -->
                    <!--
                    <xsl:otherwise>
                        <xsl:if test = "local-name(./n0:value/child::node()) = 'string'">
                            <xsl:element name = "{$elName}" namespace = "{$el_ns}"/>
                        </xsl:if>
                    </xsl:otherwise>
                    -->
                </xsl:choose>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

</xsl:stylesheet>
