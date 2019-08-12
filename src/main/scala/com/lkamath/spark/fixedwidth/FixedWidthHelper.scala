package com.lkamath.spark.fixedwidth

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object FixedWidthHelper {
  case class FixedWidthFileMetadata(encoding: String,
                                    recordSeparator: String,
                                    paddingChar: String,
                                    commentChar: String,
                                    columns: List[FixedWidthColumn],
                                    skipLines: Int,
                                    headersPresent: Boolean = true,
                                    maxRowLength: Int = 10000) {
    def getContiguousColumns(): List[FixedWidthColumn] = {
      // Sort fixed width columns by starPos
      val fixedWidthColumns = this.columns.sortBy(f ⇒ f.startPos)
      val contiguousColumns = List
        .tabulate(fixedWidthColumns.length) { index ⇒
          contiguousHelper(index, fixedWidthColumns)
        }
        .flatten
        .sortBy(f ⇒ f.startPos)
      // If first column doesn't begin with startPos=1, add dummy-col with startPos=1
      if (contiguousColumns(0).startPos != 1) {
        (FixedWidthColumn(s"dummy-col0", 1, contiguousColumns(0).startPos - 1) +: contiguousColumns)
          .sortBy(f ⇒ f.startPos)
      } else {
        contiguousColumns
      }
    }

    def getMaxRowLength(): Int = {
      this.columns.reverse match {
        case last :: _ ⇒ {
          val rowLength = last.startPos + last.fieldWidth
          rowLength match {
            case _ if rowLength < 0 ⇒ this.maxRowLength
            case _ if rowLength == Int.MaxValue ⇒ this.maxRowLength
            case _ if rowLength > this.maxRowLength ⇒ this.maxRowLength
            case _ ⇒ rowLength
          }
        }
        case Nil ⇒
          throw new IllegalArgumentException(
            s"Fixed Width Columns must have at least one column"
          )
      }
    }

    def getDummyColumnIndicesAsString(): String = {
      val contiguousColumns = this.getContiguousColumns()
      contiguousColumns.zipWithIndex
        .map {
          case (c, i) => {
            if (c.colName.contains("dummy-col")) {
              i
            } else {
              -1
            }
          }
        }
        .filter(_ >= 0)
        .toList
        .mkString(",")
    }

    def getColumnWidthsAsString(): String = {
      this.getContiguousColumns().map(col ⇒ col.fieldWidth).mkString(",")
    }

    private def contiguousHelper(
      index: Int,
      fixedWidthColumns: List[FixedWidthColumn]
    ): List[FixedWidthColumn] = {
      val currentColumn = fixedWidthColumns(index)
      if (index < fixedWidthColumns.length - 1) {
        val nextStartPos = fixedWidthColumns(index + 1).startPos
        // Columns are contiguous if currentWidth+currentStartPos is equal to the next expected startPos
        if (currentColumn.fieldWidth + currentColumn.startPos == nextStartPos) {
          List(currentColumn)
        } else {
          // Columns are non contiguous. Add current column and dummy column
          List(
            currentColumn,
            FixedWidthColumn(
              s"dummy-col${index + 1}",
              currentColumn.fieldWidth + currentColumn.startPos,
              nextStartPos - (currentColumn.fieldWidth + currentColumn.startPos)
            )
          )
        }
      } else {
        // Index is now at last column. Add last of the user provided columns to list of contiguousColumns.
        List(currentColumn)
      }
    }
  }

  case class FixedWidthColumn(colName: String, startPos: Int, fieldWidth: Int)

  def createDataFrame(sparkSession: SparkSession,
                      metadata: FixedWidthFileMetadata,
                      filePath: String,
                      optionsMap: Map[String, String] = Map.empty,
                      inferSchema: Boolean = false): DataFrame = {
    if (inferSchema) {
      throw new Exception(
        "inferSchema option cannot be set to True in this version because dataframe cannot be materialized."
      )
    }

    val stringifiedSchema = StructType(
      metadata.columns.map(c => StructField(c.colName, StringType, true))
    )
    val df = sparkSession.read
      .format("fixed")
      .option("maxRowLength", metadata.maxRowLength.toString)
      .option("widths", metadata.getColumnWidthsAsString())
      .option("padding", metadata.paddingChar)
      .option("recordSeparator", metadata.recordSeparator)
      .option("comment", metadata.commentChar)
      .option("encoding", metadata.encoding)
      .option("skipLines", metadata.skipLines)
      .option("dummyColumnIndices", metadata.getDummyColumnIndicesAsString())
      .options(optionsMap)
      .schema(stringifiedSchema)
      .load(filePath)

    val userColumnNames = metadata.columns.map(c => c.colName)
    // Select only those columns that are chosen by user in FixedWidthFileMetadata
    val selectDf = df.select(userColumnNames.head, userColumnNames.tail: _*)
    selectDf
  }
}
