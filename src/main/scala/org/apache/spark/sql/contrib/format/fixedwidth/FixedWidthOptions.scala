package org.apache.spark.sql.contrib.format.fixedwidth

import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.catalyst.util._

class FixedWidthOptions(@transient private val parameters: CaseInsensitiveMap[String]) extends Serializable {

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  /** Function to convert the string value of paramName to Char value
    *
    * @param paramName : Name of the parameter
    * @param default : default value of the parameter to be used if paramName is not present
    * @return Char representation of the value in paramName
    */
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Throw"))
  private def getChar(paramName: String, default: Char): Char = {
    val paramValue = parameters.get(paramName)
    paramValue match {
      case None                             ⇒ default
      case Some(null)                       ⇒ default
      case Some(value) if value.length == 0 ⇒ '\u0000'
      case Some(value) if value.length == 1 ⇒ value.charAt(0)
      case _                                ⇒ throw new RuntimeException(s"$paramName cannot be more than one character")
    }
  }

  /** Function to convert the string value of paramName to Integer value
    *
    * @param paramName : Name of the parameter
    * @param default : default value of the parameter to be used if paramName is not present
    * @return Integer representation of the value in the paramName
    */
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Throw"))
  private def getInt(paramName: String, default: Int): Int = {
    val paramValue = parameters.get(paramName)
    paramValue match {
      case None       ⇒ default
      case Some(null) ⇒ default
      case Some(value) ⇒
        try {
          value.toInt
        } catch {
          case e: NumberFormatException ⇒
            throw new RuntimeException(s"$paramName should be an integer. Found $value")
        }
    }
  }

  /** Function to convert the string value of paramName to boolean value
    *
    * @param paramName : Name of the parameter
    * @param default : default value of the parameter to be used if paramName is not present
    * @return Integer representation of the value in the paramName
    */
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Throw"))
  private def getBool(paramName: String, default: Boolean = false): Boolean = {
    val param = parameters.getOrElse(paramName, default.toString)
    if (param == null) {
      default
    } else if (param.toLowerCase(Locale.ROOT) == "true") {
      true
    } else if (param.toLowerCase(Locale.ROOT) == "false") {
      false
    } else {
      throw new Exception(s"$paramName flag can be true or false")
    }
  }

  /** Function to convert the string value of paramName to an Array
    *
    * @param paramName : Name of the parameter
    * @param default : default value of the parameter to be used if paramName is not present
    * @return List[Int] representation of the values in the paramName
    */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def getList(paramName: String, default: List[Int] = List.empty): List[Int] = {
    //Eg: if paramName="2,3,4" this function should return List(2,3,4)
    val param = parameters.getOrElse(paramName, "")
    if (param == "") {
      default
    } else {
      param.split(",").map(_.toInt).toList
    }
  }

  // Indicates the padding character used to pad fields in fixed width files
  val paddingChar = getChar("padding", ' ')

  // Indicates the encoding of a fixed width file
  val encoding = parameters.getOrElse("encoding", parameters.getOrElse("charset", StandardCharsets.UTF_8.name()))

  // Indicates the widths of columns in the fixed width file
  val widths = getList("widths", List.empty)

  // Indicates the comment character that tells what lines in the file are comments
  val comment = getChar("comment", '\u0000')

  // Indicates how many lines to skip from the beginning of the file
  val skipLines = getInt("skipLines", 0)

  // Indicates whether the file contains headers or not
  val headerFlag = getBool("header", true)

  // Indicates whether to infer file schema or not
  val inferSchemaFlag = getBool("inferSchema")

  // Indicates the character that separates two consecutive records in a file
  val lineSeparator = parameters.getOrElse("recordSeparator", "\n")

  // Indicates how a NULL is represented in the file
  val nullValue = parameters.getOrElse("nullValue", "NULL")

  val dateFormat: SimpleDateFormat =
    new SimpleDateFormat(parameters.getOrElse("dateFormat", "yyyy-MM-dd"))

  // Indicates the maximum number of characters that are  allowed in a column
  val maxCharsPerColumn = getInt("maxCharsPerColumn", 10000)

  // Indicates the maximum length of the row
  val maxRowLength = getInt("maxRowLength", 10000)

  // Indicates whether a comment character is set ot not
  val isCommentSet = this.comment != '\u0000'

  val timestampFormat: FastDateFormat =
    FastDateFormat.getInstance(parameters.getOrElse("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"), Locale.US)

  val dummyColumnIndices = getList("dummyColumnIndices")
}
