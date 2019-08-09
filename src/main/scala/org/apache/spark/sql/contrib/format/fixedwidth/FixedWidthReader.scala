package org.apache.spark.sql.contrib.format.fixedwidth

import com.univocity.parsers.fixed.{FixedWidthFields, FixedWidthParser, FixedWidthParserSettings}

/** This class instantiates a fixed width parser using the options provided
  *
  * @param params: options related to Fixed Width
  */
class FixedWidthReader(params: FixedWidthOptions) {

  private val parser: FixedWidthParser = {
    val fixedWidthFields   = new FixedWidthFields(params.widths: _*)
    val fixedWidthSettings = new FixedWidthParserSettings(fixedWidthFields)
    // Add other options here
    fixedWidthSettings.getFormat().setPadding(params.paddingChar)
    fixedWidthSettings.getFormat().setLineSeparator(params.lineSeparator)
    fixedWidthSettings.getFormat().setComment(params.comment)
    fixedWidthSettings.setNullValue(params.nullValue)
    fixedWidthSettings.setRecordEndsOnNewline(true)
    // Set maximum number of characters allowed per column
    fixedWidthSettings.setMaxCharsPerColumn(params.maxCharsPerColumn)
    // Return a fixed width parser
    new FixedWidthParser(fixedWidthSettings)
  }

  /**
    * parse a line
    *
    * @param line a String with no newline at the end
    * @return array of strings where each string is a field in the Fixed Width record
    */
  def parseLine(line: String): Array[String] = parser.parseLine(line)
}
