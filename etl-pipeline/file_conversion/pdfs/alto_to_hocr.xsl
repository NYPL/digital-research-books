<?xml version="1.0" encoding="UTF-8"?>
<!--Adapted from https://github.com/filak/hOCR-to-ALTO/blob/master/alto__hocr.xsl-->
<xsl:stylesheet version="1.0"
    xmlns="http://www.w3.org/1999/xhtml"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
    xmlns:mf="http://myfunctions" 
    xmlns:alto="http://www.loc.gov/standards/alto/ns-v2#"
  >

  <xsl:output method="xml" encoding="utf-8" doctype-system="http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd" 
  doctype-public="-//W3C//DTD XHTML 1.0 Transitional//EN" indent="yes" />
  <xsl:strip-space elements="*"/>
  <!-- Default language code - fallback value -->
  <xsl:param name="language" select="'unknown'" />


  <xsl:template match="/">
    <xsl:if test="$language != 'unknown'">
      <html xml:lang="{ $language }" lang="{ $language }">
        <xsl:apply-templates/>
      </html>
    </xsl:if>

    <xsl:if test="$language = 'unknown'">
      <html>
        <xsl:apply-templates/>
      </html>
    </xsl:if>
  </xsl:template>


  <xsl:template match="alto:Description">
    <xsl:element name="head">
      <xsl:element name="title">Image: <xsl:value-of select="alto:sourceImageInformation"/>
      </xsl:element>
      
      <xsl:element name="meta">
        <xsl:attribute name="http-equiv">Content-Type</xsl:attribute>
        <xsl:attribute name="content">text/html; charset=utf-8</xsl:attribute>
      </xsl:element>
      
      <xsl:apply-templates select="alto:OCRProcessing/alto:ocrProcessingStep"/>

      <xsl:element name="meta">
        <xsl:attribute name="name">ocr-capabilities</xsl:attribute>
        <xsl:attribute name="content">ocr_page ocr_header ocr_footer ocr_carea ocr_par ocr_line ocrx_word</xsl:attribute>
      </xsl:element>
    </xsl:element>
  </xsl:template>

  
  <xsl:template match="alto:sourceImageInformation">
      <xsl:value-of select="alto:fileName"/>
  </xsl:template>

  <xsl:template match="alto:OCRProcessing/alto:ocrProcessingStep">
    <xsl:element name="meta">
      <xsl:attribute name="name">ocr-system</xsl:attribute>
      <xsl:attribute name="content">
        <xsl:value-of select="alto:processingSoftware/alto:softwareName"/>
        <xsl:text> </xsl:text>
        <xsl:value-of select="alto:processingSoftware/alto:softwareVersion"/>
      </xsl:attribute>
    </xsl:element>
  </xsl:template>


  <xsl:template match="alto:Styles">
  </xsl:template>

  
  <xsl:template match="alto:Layout">
    <xsl:element name="body">
      <xsl:apply-templates select="alto:Page"/>
    </xsl:element>
  </xsl:template>

 
 
  <xsl:template match="alto:Page">
    <xsl:variable name="fname">
      <xsl:value-of select="/alto:alto/alto:Description/alto:sourceImageInformation/alto:fileName"/>
    </xsl:variable>

    <xsl:element name="div">
      <xsl:attribute name="class">ocr_page</xsl:attribute>
      <xsl:attribute name="id">
        <xsl:call-template name="mf:getId">
          <xsl:with-param name="ID" select="@ID"/>
          <xsl:with-param name="nodetype" select="'page'"/>
          <xsl:with-param name="node" select="."/>
        </xsl:call-template>
      </xsl:attribute>
      <xsl:attribute name="title">
        <xsl:value-of select="concat('image ', $fname, '; bbox 0 0 ', @WIDTH, ' ', @HEIGHT, '; ppageno 0')"/>
      </xsl:attribute>

      <xsl:apply-templates select="alto:TopMargin"/>
      <xsl:apply-templates select="alto:PrintSpace"/>
      <xsl:apply-templates select="alto:BottomMargin"/>
    </xsl:element>
  </xsl:template>

  
  <xsl:template match="alto:TopMargin">
    <xsl:element name="div">
      <xsl:attribute name="class">ocr_header</xsl:attribute>
      <xsl:attribute name="id">
        <xsl:call-template name="mf:getId">
          <xsl:with-param name="ID" select="@ID"/>
          <xsl:with-param name="nodetype" select="'block'"/>
          <xsl:with-param name="node" select="."/>
        </xsl:call-template>
      </xsl:attribute>
      <xsl:attribute name="title">
        <xsl:call-template name="mf:getBox">
          <xsl:with-param name="HEIGHT" select="@HEIGHT"/>
          <xsl:with-param name="WIDTH" select="@WIDTH"/>
          <xsl:with-param name="VPOS" select="@VPOS"/>
          <xsl:with-param name="HPOS" select="@HPOS"/>
          <xsl:with-param name="WC" select="@WC"/>
        </xsl:call-template>
      </xsl:attribute>
      <xsl:apply-templates select="alto:ComposedBlock"/>
      <xsl:apply-templates select="alto:TextBlock"/>
    </xsl:element>
  </xsl:template>
  

  <xsl:template match="alto:PrintSpace">
      <xsl:apply-templates select="alto:ComposedBlock"/>
      <xsl:apply-templates select="alto:TextBlock"/>
  </xsl:template>
  
    
  <xsl:template match="alto:BottomMargin">
    <xsl:element name="div">
      <xsl:attribute name="class">ocr_footer</xsl:attribute>
      <xsl:attribute name="id">
        <xsl:call-template name="mf:getId">
          <xsl:with-param name="ID" select="@ID"/>
          <xsl:with-param name="nodetype" select="'block'"/>
          <xsl:with-param name="node" select="."/>
        </xsl:call-template>
      </xsl:attribute>
      <xsl:attribute name="title">
        <xsl:call-template name="mf:getBox">
          <xsl:with-param name="HEIGHT" select="@HEIGHT"/>
          <xsl:with-param name="WIDTH" select="@WIDTH"/>
          <xsl:with-param name="VPOS" select="@VPOS"/>
          <xsl:with-param name="HPOS" select="@HPOS"/>
          <xsl:with-param name="WC" select="@WC"/>
        </xsl:call-template>
      </xsl:attribute>
      <xsl:apply-templates select="alto:ComposedBlock"/>
      <xsl:apply-templates select="alto:TextBlock"/>
    </xsl:element>
  </xsl:template>
  
  
  <xsl:template match="alto:ComposedBlock">
    <xsl:element name="div">
      <xsl:attribute name="class">ocr_carea</xsl:attribute>
      <xsl:attribute name="id">
        <xsl:call-template name="mf:getId">
          <xsl:with-param name="ID" select="@ID"/>
          <xsl:with-param name="nodetype" select="'block'"/>
          <xsl:with-param name="node" select="."/>
        </xsl:call-template>
      </xsl:attribute>
      <xsl:attribute name="title">
        <xsl:call-template name="mf:getBox">
          <xsl:with-param name="HEIGHT" select="@HEIGHT"/>
          <xsl:with-param name="WIDTH" select="@WIDTH"/>
          <xsl:with-param name="VPOS" select="@VPOS"/>
          <xsl:with-param name="HPOS" select="@HPOS"/>
          <xsl:with-param name="WC" select="@WC"/>
        </xsl:call-template>
      </xsl:attribute>
      <xsl:apply-templates select="alto:TextBlock|alto:ComposedBlock"/>
    </xsl:element>
  </xsl:template>


  <xsl:template match="alto:TextBlock">
    <xsl:element name="p">
      <xsl:attribute name="class">ocr_par</xsl:attribute>
      <xsl:attribute name="dir">ltr</xsl:attribute>
      <xsl:attribute name="id">
        <xsl:call-template name="mf:getId">
          <xsl:with-param name="ID" select="@ID"/>
          <xsl:with-param name="nodetype" select="'par'"/>
          <xsl:with-param name="node" select="."/>
        </xsl:call-template>
      </xsl:attribute>
      <xsl:attribute name="title">
        <xsl:call-template name="mf:getBox">
          <xsl:with-param name="HEIGHT" select="@HEIGHT"/>
          <xsl:with-param name="WIDTH" select="@WIDTH"/>
          <xsl:with-param name="VPOS" select="@VPOS"/>
          <xsl:with-param name="HPOS" select="@HPOS"/>
          <xsl:with-param name="WC" select="@WC"/>
        </xsl:call-template>
      </xsl:attribute>
      <xsl:variable name="lang" select="@language|@LANG"/>
      <xsl:if test="$lang != ''">
        <xsl:attribute name="lang">
          <xsl:value-of select="$lang"/>
        </xsl:attribute>
      </xsl:if>
      <xsl:if test="$lang = '' and $language != 'unknown'">
        <xsl:attribute name="lang">
          <xsl:value-of select="$language"/>
        </xsl:attribute>
      </xsl:if>
      <xsl:apply-templates select="alto:TextLine"/>
    </xsl:element>
  </xsl:template>


  <xsl:template match="alto:TextLine">
    <xsl:element name="span">
      <xsl:attribute name="class">ocr_line</xsl:attribute>
      <xsl:attribute name="id">
        <xsl:call-template name="mf:getId">
          <xsl:with-param name="ID" select="@ID"/>
          <xsl:with-param name="nodetype" select="'line'"/>
          <xsl:with-param name="node" select="."/>
        </xsl:call-template>
      </xsl:attribute>
      <xsl:attribute name="title">
        <xsl:call-template name="mf:getBox">
          <xsl:with-param name="HEIGHT" select="@HEIGHT"/>
          <xsl:with-param name="WIDTH" select="@WIDTH"/>
          <xsl:with-param name="VPOS" select="@VPOS"/>
          <xsl:with-param name="HPOS" select="@HPOS"/>
          <xsl:with-param name="WC" select="@WC"/>
        </xsl:call-template>
      </xsl:attribute>
      <xsl:apply-templates select="alto:String"/>
    </xsl:element>
  </xsl:template>


  <xsl:template match="alto:String">
    <xsl:variable name="textstyleid">
      <xsl:value-of select="@STYLEREFS"/>
    </xsl:variable>
    
    <xsl:variable name="fontfamily">
      <xsl:value-of select="//alto:alto/alto:Styles/alto:TextStyle[@ID=$textstyleid]/@FONTFAMILY" />
    </xsl:variable>
    
    <xsl:variable name="fontsize">
      <xsl:value-of select="//alto:alto/alto:Styles/alto:TextStyle[@ID=$textstyleid]/@FONTSIZE" />
    </xsl:variable>
    
    <xsl:choose>
      <xsl:when test="$textstyleid != ''">
        <xsl:element name="span">
          <xsl:attribute name="class">ocrx_word</xsl:attribute>
          <xsl:attribute name="id">
            <xsl:call-template name="mf:getId">
              <xsl:with-param name="ID" select="@ID"/>
              <xsl:with-param name="nodetype" select="'word'"/>
              <xsl:with-param name="node" select="."/>
            </xsl:call-template>
          </xsl:attribute>
          <xsl:attribute name="title">
            <xsl:call-template name="mf:getBox">
              <xsl:with-param name="HEIGHT" select="@HEIGHT"/>
              <xsl:with-param name="WIDTH" select="@WIDTH"/>
              <xsl:with-param name="VPOS" select="@VPOS"/>
              <xsl:with-param name="HPOS" select="@HPOS"/>
              <xsl:with-param name="WC" select="@WC"/>
            </xsl:call-template>
          </xsl:attribute>
          <xsl:attribute name="x_font">
            <xsl:value-of select="$fontfamily"/>
          </xsl:attribute>
          <xsl:attribute name="x_fsize">
            <xsl:value-of select="$fontsize"/>
          </xsl:attribute>
          <xsl:call-template name="style_and_content"/>
        </xsl:element>
      </xsl:when>
      <xsl:otherwise>
        <xsl:element name="span">
          <xsl:attribute name="class">ocrx_word</xsl:attribute>
          <xsl:attribute name="id">
            <xsl:call-template name="mf:getId">
              <xsl:with-param name="ID" select="@ID"/>
              <xsl:with-param name="nodetype" select="'word'"/>
              <xsl:with-param name="node" select="."/>
            </xsl:call-template>
          </xsl:attribute>
          <xsl:attribute name="title">
            <xsl:call-template name="mf:getBox">
              <xsl:with-param name="HEIGHT" select="@HEIGHT"/>
              <xsl:with-param name="WIDTH" select="@WIDTH"/>
              <xsl:with-param name="VPOS" select="@VPOS"/>
              <xsl:with-param name="HPOS" select="@HPOS"/>
              <xsl:with-param name="WC" select="@WC"/>
            </xsl:call-template>
          </xsl:attribute>
          <xsl:call-template name="style_and_content"/>
        </xsl:element>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  
  <xsl:template name="style_and_content">
    <xsl:choose>
      <xsl:when test="@STYLE = 'bold'">
        <strong>
            <xsl:call-template name="content"/>
        </strong>
      </xsl:when>
      <xsl:when test="@STYLE = 'italics'">
        <em>
            <xsl:call-template name="content"/>
        </em>
      </xsl:when>
      <xsl:when test="@STYLE = 'subscript'">
        <sub>
            <xsl:call-template name="content"/>
        </sub>
      </xsl:when>
      <xsl:when test="@STYLE = 'superscript'">
        <sup>
            <xsl:call-template name="content"/>
        </sup>
      </xsl:when>
      <xsl:when test="@STYLE = 'underline'">
        <u>
            <xsl:call-template name="content"/>
        </u>
      </xsl:when>
      <xsl:when test="@STYLE = 'smallcaps'">
        <span class="small-caps">
            <xsl:call-template name="content"/>
        </span>
      </xsl:when>
      <xsl:otherwise>
          <xsl:call-template name="content"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
  

  <xsl:template name="content">
    <xsl:choose>
      <xsl:when test="@CONTENT != ''">
        <xsl:value-of select="@CONTENT"/>
        <xsl:if test="local-name(following-sibling::*[1]) = 'HYP'">
             <xsl:text>-</xsl:text>
         </xsl:if>
      </xsl:when>

      <xsl:otherwise>
            <xsl:text>
            </xsl:text>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
  

  <xsl:template name="mf:getBox">
    <xsl:param name="HEIGHT"/>
    <xsl:param name="WIDTH"/>
    <xsl:param name="VPOS"/>
    <xsl:param name="HPOS"/>
    <xsl:param name="WC"/>

    <xsl:variable name="right" select="number($WIDTH) + number($HPOS)"/>
    <xsl:variable name="bottom" select="number($HEIGHT) + number($VPOS)"/>

    <xsl:choose>
      <xsl:when test="$WC != ''">
        <xsl:variable name="wconf" select="number($WC) * 100"/>
        <xsl:variable name="wconfString" select="concat('; x_wconf ', string($wconf))"/>
        <xsl:value-of select="concat('bbox ', $HPOS, ' ', $VPOS, ' ', string($right), ' ', string($bottom), ' ', $wconfString)"/>
      </xsl:when>

      <xsl:otherwise>
        <xsl:value-of select="concat('bbox ', $HPOS, ' ', $VPOS, ' ', string($right), ' ', string($bottom))"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>


  <xsl:template name="mf:getId">
    <xsl:param name="ID"/>
    <xsl:param name="nodetype"/>
    <xsl:param name="node"/>
    
    <xsl:choose>
      <xsl:when test="$ID != ''">
        <xsl:value-of select="$ID"/>
      </xsl:when>

      <xsl:otherwise>
        <xsl:value-of select="concat($nodetype, '_', generate-id($node))"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  
</xsl:stylesheet>
