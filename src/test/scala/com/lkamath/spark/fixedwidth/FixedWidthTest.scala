package com.lkamath.spark.fixedwidth

import com.lkamath.spark.fixedwidth.FixedWidthHelper.{FixedWidthColumn, FixedWidthFileMetadata, createDataFrame}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.specs2.matcher.ShouldMatchers
import org.scalatest.FlatSpec

class FixedWidthTest extends FlatSpec with ShouldMatchers {

  def getFixedWidthMetadata(encoding: String = "UTF-8",
                            recordSeparator: String = "\n",
                            paddingChar: String = "#",
                            commentChar: String = "-",
                            columns: List[FixedWidthColumn],
                            skipLines: Int = 0,
                            headersPresent: Boolean = true): FixedWidthFileMetadata =
    FixedWidthFileMetadata(
      encoding,
      recordSeparator,
      paddingChar,
      commentChar,
      columns,
      skipLines,
      headersPresent
    )

  def runFixedWidthTest(metadata: FixedWidthFileMetadata, filePath: String, numRows: Int, numCols: Int, inferSchema: Boolean = false): DataFrame = {
    val df = createDataFrame(sparkSession, metadata, filePath, inferSchema = inferSchema)
    assert(df.count() == numRows)
    assert(df.columns.length == numCols)
    df
  }

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("fixed-width-test")
    .set("spark.ui.enabled", "false")
    .set("spark.sql.crossJoin.enabled", "true")
    .set("spark.sql.catalogImplementation", "in-memory")
    .set("spark.sql.retainGroupColumns", "false")
  val sparkSession = SparkSession.builder().config(conf).getOrCreate()

  "fixed width " should "support right aligned fixed width file" in {

    val columns = List(FixedWidthColumn("Bank_Name", 1, 37), FixedWidthColumn("City", 38, 15),
      FixedWidthColumn("ST", 53, 2), FixedWidthColumn("CERT", 55, 4))
    val metadata      = getFixedWidthMetadata(commentChar = "*", columns = columns)
    val fixedWidthUrl = this.getClass.getResource("/banks-right-aligned.txt").getPath
    runFixedWidthTest(metadata, fixedWidthUrl, 9, 4)
  }

  "fixed width " should "support left aligned fixed width with headers, comments" in {
    val columns = List(
      FixedWidthColumn("column1", 1, 11),
      FixedWidthColumn("column2", 12, 9),
      FixedWidthColumn("column3", 21, 19),
      FixedWidthColumn("column4", 40, 10),
      FixedWidthColumn("col5", 50, 6),
      FixedWidthColumn("col6", 56, 16)
    )
    val metadata      = getFixedWidthMetadata(paddingChar = "-", commentChar = "/", columns = columns)
    val fixedWidthUrl = this.getClass.getResource("/users-fixed-width.txt").getPath
    runFixedWidthTest(metadata, fixedWidthUrl, 4, 6)
  }

  "fixed width " should "support right aligned fixed width with headers and comments and skip empty lines" in {
    val columns = List(
      FixedWidthColumn("column1", 1, 11),
      FixedWidthColumn("column2", 12, 9),
      FixedWidthColumn("column3", 21, 19),
      FixedWidthColumn("column4", 40, 10),
      FixedWidthColumn("col5", 50, 6),
      FixedWidthColumn("col6", 56, 16)
    )
    val metadata      = getFixedWidthMetadata(paddingChar = "-", commentChar = "/", columns = columns)
    val fixedWidthUrl = this.getClass.getResource("/users-fixed-width-right-aligned-with-nulls.txt").getPath
    runFixedWidthTest(metadata, fixedWidthUrl, 4, 6)
  }

  "fixed width " should "parse non-contiguous dataset skipping first and last columns" in {
    val columns = List(
      FixedWidthColumn("City", 92, 18),
      FixedWidthColumn("ST", 110, 2),
      FixedWidthColumn("CERT", 112, 5),
      FixedWidthColumn("Acquiring_Institution", 117, 66)
    )
    val metadata      = getFixedWidthMetadata(columns = columns)
    val fixedWidthUrl = this.getClass.getResource("/banks-fixed-width.txt").getPath
    runFixedWidthTest(metadata, fixedWidthUrl, 532, 4)
  }

  "fixed width " should "parse non-contiguous dataset skipping the middle columns" in {
    val columns = List(
      FixedWidthColumn("Bank_Name", 1, 91),
      FixedWidthColumn("Acquiring_Institution", 117, 66)
    )
    val metadata      = getFixedWidthMetadata(columns = columns)
    val fixedWidthUrl = this.getClass.getResource("/banks-fixed-width.txt").getPath
    runFixedWidthTest(metadata, fixedWidthUrl, 532, 2)
  }

  "fixed width " should "parse non-contiguous dataset skipping first N lines" in {
    val columns = List(
      FixedWidthColumn("Bank_Name", 1, 91),
      FixedWidthColumn("City", 92, 18),
      FixedWidthColumn("CloseDate", 183, 9)
    )
    val metadata      = getFixedWidthMetadata(columns = columns, skipLines = 100)
    val fixedWidthUrl = this.getClass.getResource("/banks-fixed-width.txt").getPath
    runFixedWidthTest(metadata, fixedWidthUrl, 432, 3)
  }

  "fixed width " should " be able to infer schema of a fixed-width file" in {
    val columns = List(
      FixedWidthColumn("Bank_Name", 1, 91),
      FixedWidthColumn("City", 92, 18),
      FixedWidthColumn("CERT", 112, 5)
    )
    val metadata = getFixedWidthMetadata(columns = columns)
    val fixedWidthUrl = this.getClass.getResource("/banks-fixed-width.txt").getPath

    val expectedSchema = StructType(
      List(
        StructField("Bank_Name", StringType, true),
        StructField("City", StringType, true),
        StructField("CERT", IntegerType, true)
      )
    )
    val df = runFixedWidthTest(metadata, fixedWidthUrl, 532, 3, inferSchema = true)
    assert(df.schema == expectedSchema)
  }
}

