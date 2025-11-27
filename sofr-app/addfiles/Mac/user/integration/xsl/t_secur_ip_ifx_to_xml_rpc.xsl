<?xml version="1.0" encoding="UTF-8"?>
<!-- Преобразование интеграционного xml-сообщения IFX в XML RPC -->
<!-- Автор: Карпов В.А. -->

<xsl:stylesheet
    xmlns:xsl = "http://www.w3.org/1999/XSL/Transform"
    xmlns:sch = "http://www.softlab.ru/xml-rpc/schema"
    xmlns:xs = "http://www.w3.org/2001/XMLSchema"
    xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns = "http://www.softlab.ru/xml-rpc/schema"
    version="1.0">

    <xsl:output method = "xml" indent = "yes" omit-xml-declaration = "yes"/>
    <xsl:strip-space elements = "*"/>

    <!-- Глобальная переменная с наименованием сервиса -->
    <xsl:variable name = "serv_name" select = "local-name(/child::node()[1])"/>

    <!-- Глобальная переменная с описанием объекта сервиса -->
    <xsl:variable name = "root_element" select = "document('./s_in_rs_secur_xml_schema.xsd')/xs:schema/xs:element[@name = $serv_name]"/>

    <!-- Обработка начинается с применения этого шаблона -->
    <xsl:template match = "/">
        <!-- Шаблон формирования обязательных выражений -->
        <!-- Стандартная для XML инструкция -->
        <!--
        <xsl:text disable-output-escaping = "yes"><![CDATA[<?xml version="1.0" encoding="windows-1251"?>]]></xsl:text>
        -->
        <!-- Основной раздел XML для RS-Bank --> <!-- Без reqId не будет работать ConvertToRSL() -->
        <!--
        <sch:methodResponse sch:reqId = "">
            <xsl:apply-templates select = "node()[1]" mode = "start"/>
        </sch:methodResponse>
        -->
        <sch:methodCall
            sch:oper = "99999"
            sch:reqId = ""
            sch:source = "">
            <sch:methodName>
                <xsl:value-of select = "concat('RunMacro.', $serv_name, '.', $serv_name)"/>
            </sch:methodName>
            <xsl:apply-templates select = "node()[1]" mode = "start"/>
        </sch:methodCall>
    </xsl:template>

    <xsl:template match = "node()[1]" mode = "start">
        <sch:params>
            <sch:param>
                <sch:value>
                    <xsl:apply-templates select = "self::node()" mode = "body">
                        <xsl:with-param name = "TegName" select = "$serv_name"/>
                        <xsl:with-param name = "xsd" select = "$root_element"/>
                    </xsl:apply-templates>
                </sch:value>
            </sch:param>
            <sch:param>
                <sch:value>
                    <sch:string>
                        <xsl:value-of select = "$serv_name"/>
                    </sch:string>
                </sch:value>
            </sch:param>
        </sch:params>
    </xsl:template>

    <xsl:template match = "node()" mode = "body">
        <xsl:param name = "xsd"/>
        <xsl:choose>
            <xsl:when test = "not($xsd/@type)">
                <xsl:choose>
                    <xsl:when test = "not($xsd/@ref)">
                        <xsl:variable name = "env_name" select = "local-name($xsd/child::node()[local-name(self::node()) = 'restriction'
                                                                                             or local-name(self::node()) = 'sequence'
                                                                                             or local-name(self::node()) = 'all'
                                                                                             or local-name(self::node()) = 'complexType'
                                                                                             or local-name(self::node()) = 'simpleType'
                                                                                             or local-name(self::node()) = 'union'])"/>
                        <xsl:choose>
                            <xsl:when test = "$env_name = 'restriction'">
                                <xsl:call-template name = "type_choose">
                                    <xsl:with-param name = "type_name" select = "substring(substring-after(concat(':', $xsd/xs:restriction/@base), substring-before($xsd/xs:restriction/@base, ':')), 2)"/>
                                </xsl:call-template>
                            </xsl:when>
                            <xsl:when test = "$env_name = 'union'">
                                <xsl:choose>
                                    <!-- Один тип данных -->
                                    <xsl:when test = "not(contains($xsd/xs:union/@memberTypes, ' '))">
                                        <xsl:call-template name = "type_choose">
                                            <xsl:with-param name = "type_name" select = "substring(substring-after(concat(':', $xsd/xs:union/@memberTypes), substring-before($xsd/xs:union/@memberTypes, ':')), 2)"/>
                                        </xsl:call-template>
                                    </xsl:when>
                                    <!-- С otherwise все усложняется -->
                                </xsl:choose>
                            </xsl:when>
                            <xsl:when test = "$env_name = 'sequence' or $env_name = 'all'">
                                <xsl:choose>
                                    <xsl:when test = "count($xsd/child::node()/xs:element) = 1">
                                        <xsl:variable name = "max_occurs" select = "$xsd/child::node()/xs:element/@maxOccurs"/>
                                        <xsl:choose>
                                            <xsl:when test = "$max_occurs = 'unbounded' or $max_occurs > 1">
                                                <xsl:call-template name = "array_obj">
                                                    <xsl:with-param name = "xsd" select = "$xsd"/>
                                                </xsl:call-template>
                                            </xsl:when>
                                            <xsl:otherwise>
                                                <xsl:call-template name = "struct_obj">
                                                    <xsl:with-param name = "xsd" select = "$xsd"/>
                                                </xsl:call-template>
                                            </xsl:otherwise>
                                        </xsl:choose>
                                    </xsl:when>
                                    <xsl:otherwise>
                                        <xsl:call-template name = "struct_obj">
                                            <xsl:with-param name = "xsd" select = "$xsd"/>
                                        </xsl:call-template>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </xsl:when>
                            <xsl:when test = "$env_name = 'complexType' or $env_name = 'simpleType'">
                                <xsl:apply-templates select = "self::node()" mode = "body">
                                    <xsl:with-param name = "xsd" select = "$xsd/child::node()"/>
                                </xsl:apply-templates>
                            </xsl:when>
                        </xsl:choose>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:apply-templates select = "self::node()" mode = "body">
                            <xsl:with-param name = "xsd" select = "document('./s_in_rs_secur_xml_schema.xsd')/xs:schema/xs:element[@name = $xsd/@ref]"/>
                        </xsl:apply-templates>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:when>
            <xsl:otherwise>
                <xsl:call-template name = "type_choose">
                    <xsl:with-param name = "type_name" select = "substring(substring-after(concat(':', $xsd/@type), substring-before($xsd/@type, ':')), 2)"/>
                </xsl:call-template>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template name = "type_choose">
        <xsl:param name = "type_name"/>
        <xsl:choose>
            <xsl:when test = "$type_name = 'string'">
                <xsl:call-template name = "string_type"/>
            </xsl:when>
            <xsl:when test = "$type_name = 'date'">
                <xsl:call-template name = "date_type"/>
            </xsl:when>
            <xsl:when test = "$type_name = 'time'">
                <xsl:call-template name = "time_type"/>
            </xsl:when>
            <xsl:when test = "$type_name = 'boolean'">
                <xsl:call-template name = "bool_type"/>
            </xsl:when>
            <xsl:when test = "$type_name = 'int'">
                <xsl:call-template name = "int_type"/>
            </xsl:when>
            <xsl:when test = "$type_name = 'integer'">
                <xsl:call-template name = "int_type"/>
            </xsl:when>
            <xsl:when test = "$type_name = 'long'">
                <xsl:call-template name = "int_type"/>
            </xsl:when>
            <xsl:when test = "$type_name = 'dateTime'">
                <xsl:call-template name = "timestamp_type"/>
            </xsl:when>
            <xsl:when test = "$type_name = 'money'">
                <xsl:call-template name = "money_type"/>
            </xsl:when>
            <xsl:when test = "$type_name = 'float'">
                <xsl:call-template name = "double_type"/>
            </xsl:when>
            <xsl:when test = "$type_name = 'double'">
                <xsl:call-template name = "double_type"/>
            </xsl:when>
            <xsl:when test = "$type_name = 'decimal'">
                <xsl:call-template name = "double_type"/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:apply-templates select = "self::node()" mode = "body">
                    <xsl:with-param name = "xsd" select = "document('./s_in_rs_secur_xml_schema.xsd')/xs:schema/child::node()[local-name(self::node()) = 'complexType'
                                                                                                                           or local-name(self::node()) = 'simpleType'][@name = $type_name]"/>
                </xsl:apply-templates>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- Шаблон преобразования массива -->
    <xsl:template name = "array_obj">
        <xsl:param name = "xsd"/>
        <sch:array>
            <sch:data>
                <xsl:for-each select = "./child::node()">
                    <xsl:variable name = "teg_name" select = "local-name(.)"/>
                    <sch:value>
                        <xsl:apply-templates select = "self::node()" mode = "body">
                            <xsl:with-param name = "xsd" select = "$xsd/child::node()/xs:element[@name = $teg_name]"/>
                        </xsl:apply-templates>
                    </sch:value> 
                </xsl:for-each>
            </sch:data>
        </sch:array>
    </xsl:template>

    <!-- Шаблон преобразования структуры -->
    <xsl:template name = "struct_obj">
        <xsl:param name = "xsd"/>
        <xsl:choose>
            <xsl:when test = "not(local-name(./child::node()))">
                <!--
                <sch:struct>
                    <sch:member>
                        <sch:name>EmptyStruct</sch:name>
                        <sch:value><NULL/></sch:value>
                    </sch:member>
                </sch:struct>
                -->
                <NULL/>
            </xsl:when>
            <xsl:otherwise>
                <sch:struct>
                    <xsl:for-each select = "./child::node()">
                        <xsl:variable name = "teg_name" select = "local-name(.)"/>
                        <sch:member>
                            <sch:name>
                                <xsl:value-of select = "$teg_name"/>
                            </sch:name>
                            <sch:value>
                                <xsl:choose>
                                <xsl:when test="$xsd/child::node()/xs:element[@name = $teg_name]">
                                    <xsl:apply-templates select = "self::node()" mode = "body">
                                        <xsl:with-param name = "xsd" select = "$xsd/child::node()/xs:element[@name = $teg_name]"/>
                                    </xsl:apply-templates>
                                </xsl:when>
                                    <xsl:otherwise>
                                        <!-- Это должно навести на мысль, что в схеме элемент отсутствует -->
                                        <NULL/>
                                        <!-- Это решение может привести к неактуальности схемы -->
                                        <!--<xsl:call-template name = "string_type"/>-->
                                    </xsl:otherwise>
                                </xsl:choose>
                            </sch:value>
                        </sch:member>
                    </xsl:for-each>
                </sch:struct>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- Шаблон преобразования параметров с типом Строка -->
    <xsl:template name = "string_type">
        <xsl:choose>
            <xsl:when test = "string(.) != '' ">
                <sch:string>
                    <xsl:value-of select = "."/>
                </sch:string>
            </xsl:when>
            <xsl:otherwise>
                <NULL/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- Шаблон преобразования параметров с типом Дата -->
    <xsl:template name = "date_type">
        <xsl:choose>
            <xsl:when test = "string(.) != '' ">
                <sch:date>
                    <xsl:value-of select = "."/>
                </sch:date>
            </xsl:when>
            <xsl:otherwise>
                <NULL/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- Шаблон преобразования параметров с типом Время -->
    <xsl:template name = "time_type">
        <xsl:choose>
            <xsl:when test = "string(.) != '' ">
                <sch:time>
                    <xsl:value-of select = "."/>
                </sch:time>
            </xsl:when>
            <xsl:otherwise>
                <NULL/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- Шаблон преобразования параметров с типом Bool -->
    <xsl:template name = "bool_type">
        <xsl:choose>
            <xsl:when test = "string(.) != '' ">
                <sch:boolean>
                    <xsl:value-of select = "translate(.,'AEFLRSTU','aeflrstu')"/>
                </sch:boolean>
            </xsl:when>
            <xsl:otherwise>
                <NULL/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- Шаблон преобразования параметров с типом Integer -->
    <xsl:template name = "int_type">
        <xsl:choose>
            <xsl:when test = "string(.) != '' ">
                <sch:integer>
                    <xsl:value-of select = "."/>
                </sch:integer>
            </xsl:when>
            <xsl:otherwise>
                <NULL/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- Шаблон преобразования параметров с типом Money -->
    <xsl:template name = "money_type">
        <xsl:choose>
            <xsl:when test = "string(.) != '' ">
                <sch:money>
                    <xsl:value-of select = "."/>
                </sch:money>
            </xsl:when>
            <xsl:otherwise>
                <NULL/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- Шаблон преобразования параметров с типом Double -->
    <xsl:template name = "double_type">
        <xsl:choose>
            <xsl:when test = "string(.) != '' ">
                <sch:double>
                    <xsl:value-of select = "."/>
                </sch:double>
            </xsl:when>
            <xsl:otherwise>
                <NULL/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- Шаблон преобразования параметров с типом Timestamp -->
    <xsl:template name = "timestamp_type">
        <xsl:choose>
            <xsl:when test = "string(.) != '' ">
                <sch:timestamp>
                    <!--<xsl:value-of select = "."/>-->
                    <xsl:value-of select = "translate(normalize-space(.), ' ', 'T')"/>
                </sch:timestamp>
            </xsl:when>
            <xsl:otherwise>
                <NULL/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

</xsl:stylesheet>