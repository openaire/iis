<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:dct="http://datacite.org/schema/kernel-2.2"
    xmlns:dnet="eu.dnetlib.actionmanager.actions.infopackage.DataciteInfoPackageToHbaseXsltFunctions"
    xmlns:oaf="http://namespace.openaire.eu/oaf"
    xmlns:dri="http://www.driver-repository.eu/namespace/dri"
    xmlns:dr="http://www.driver-repository.eu/namespace/dr" xmlns:exslt="http://exslt.org/common"
    extension-element-prefixes="exslt" exclude-result-prefixes="xsl dct dnet exslt oaf dr dri">

    <xsl:output omit-xml-declaration="yes" indent="yes"/>

    <xsl:param name="trust" select="string('0.9')"/>
    <xsl:param name="provenance" select="string('UNKNOWN')"/>
    <xsl:param name="namespaceprefix" select="string('datacite')"/>

    <xsl:template match="/*">
        <xsl:variable name="metadata" select="//dct:resource"/>
        <xsl:variable name="rightNSPrefix">
            <xsl:choose>
                <xsl:when test="not($namespaceprefix)">
                    <xsl:value-of select="//oaf:datasourceprefix"/>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="$namespaceprefix"/>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:variable>

        <xsl:choose>
            <xsl:when test="count($metadata) =  0">
                <ACTIONS/>
            </xsl:when>
            <xsl:otherwise>
                <xsl:variable name="originalId" select="//dct:identifier"/>
                <xsl:variable name="resultId" select="dnet:oafSimpleId('result', //dri:objIdentifier)"/>

                <xsl:variable name="creators" select="//dct:creator"/>
                <xsl:variable name="titles" select="//dct:title"/>
                <xsl:variable name="subjects" select="//dct:subject"/>
                <xsl:variable name="publisher" select="//dct:publisher"/>
                <xsl:variable name="descriptions" select="//dct:description"/>
                <xsl:variable name="dates" select="//dct:date"/>
                <xsl:variable name="resourceType" select="//dct:resourceType"/>
                <xsl:variable name="formats" select="//dct:format"/>
                <xsl:variable name="sizes" select="//dct:size"/>
                <xsl:variable name="rights" select="//dct:rights"/>
                <xsl:variable name="version" select="//dct:version"/>
                <xsl:variable name="instanceURI"
                    select="concat('http://dx.doi.org', '/', //dct:resource/dct:identifier)"/>
                <xsl:variable name="hostedbyid"
                    select="dnet:oafSplitId('datasource', //oaf:hostedBy/@id)"/>
                <xsl:variable name="hostedbyname" select="//oaf:hostedBy/@name"/>
                <xsl:variable name="collectedfromid"
                    select="dnet:oafSplitId('datasource', //oaf:collectedFrom/@id)"/>
                <xsl:variable name="collectedfromname" select="//oaf:collectedFrom/@name"/>
                <xsl:variable name="dateOfCollection" select="//dr:dateOfCollection"/>
                <xsl:variable name="language" select="//dct:language"/>

                <ACTIONS>
                    <ACTION targetKey="{$resultId}" targetColumnFamily="result"
                        targetColumn="body">
                        <xsl:value-of
                            select="dnet:oafDataCiteResultFromInfoPackage($resultId, $metadata, $titles, 
                            $subjects, $publisher, $descriptions, $dates, $resourceType, 
                            $formats, $sizes, $language, $rights, $version, $provenance, $trust, $hostedbyid, $hostedbyname,
                            $collectedfromid, $collectedfromname, $originalId, $instanceURI, $dateOfCollection)"
                        />
                    </ACTION>

                    <xsl:for-each select="//dct:creator">
                        <xsl:variable name="personIdTemp">
                            <xsl:choose>
                                <xsl:when test="string-length(./dct:nameIdentifier) &gt; 0">
                                    <xsl:value-of
                                        select="translate(normalize-space(./dct:nameIdentifier),' .,','___')"
                                    />
                                </xsl:when>
                                <xsl:otherwise>
                                    <xsl:value-of
                                        select="translate(normalize-space(./dct:creatorName),' .,','___')"
                                    />
                                </xsl:otherwise>
                            </xsl:choose>

                        </xsl:variable>
                        
                        <xsl:variable name="personId" select="dnet:oafId('person', $namespaceprefix, concat($originalId, '::', normalize-space($personIdTemp)))"/>
                        
                        <xsl:variable name="originalPersonId" select="./dct:nameIdentifier"/>
                        <xsl:if test="string-length($personId) &gt; 0">
                            <ACTION targetKey="{$personId}" targetColumnFamily="person" targetColumn="body">                                
                                <xsl:value-of
                                    select="dnet:oafDataCitePersonFromInfoPackage($personId, normalize-space(./dct:creatorName), $provenance, $trust, $collectedfromid, $collectedfromname,$originalPersonId, $dateOfCollection)"
                                />
                            </ACTION>
                            <ACTION targetKey="{$personId}" targetColumnFamily="personResult"
                                targetColumn="{$resultId}">
                                <xsl:value-of
                                    select="dnet:oafDataCitePersonResultFromInfoPackage($personId, $resultId, position(), $provenance, $trust)"
                                />
                            </ACTION>
                            <ACTION targetKey="{$resultId}" targetColumnFamily="personResult"
                                targetColumn="{$personId}">
                                <xsl:value-of
                                    select="dnet:oafDataCitePersonResultFromInfoPackage($resultId, $personId, position(), $provenance, $trust)"
                                />
                            </ACTION>
                        </xsl:if>
                    </xsl:for-each>
                </ACTIONS>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>
</xsl:stylesheet>
