package org.apache.spark.sql.contrib.format.fixedwidth

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class FixedWidthUtils {
  def readText(sparkSession: SparkSession, options: FixedWidthOptions, location: String): RDD[String] =
    if (Charset.forName(options.encoding) == StandardCharsets.UTF_8) {
      sparkSession.sparkContext.textFile(location)
    } else {
      val charset = options.encoding
      sparkSession.sparkContext
        .hadoopFile[LongWritable, Text, TextInputFormat](location)
        .mapPartitions(_.map(pair ⇒ new String(pair._2.getBytes, 0, pair._2.getLength, charset)))
    }

  /**
    * Returns a RDD for the files in the input path
    */
  def baseRdd(sparkSession: SparkSession, options: FixedWidthOptions, inputPaths: Seq[String]): RDD[String] =
    readText(sparkSession, options, inputPaths.mkString(","))

  /**
    * Returns the first line of the first non-empty file in path
    */
  def findFirstLine(options: FixedWidthOptions, rdd: RDD[String]): String =
    if (options.isCommentSet) {
      val comment = options.comment.toString
      rdd
        .filter { line ⇒
          line.trim.nonEmpty && !line.startsWith(comment)
        }
        .first()
    } else {
      rdd
        .filter { line ⇒
          line.trim.nonEmpty
        }
        .first()
    }

  /**
    * Returns header names that are unique for each column
    */
  def makeSafeHeader(row: Array[String], options: FixedWidthOptions, caseSensitive: Boolean): Array[String] =
    if (options.headerFlag) {
      val duplicates = {
        val headerNames = row
          .filter(_ != null)
          .map(name ⇒ if (caseSensitive) name else name.toLowerCase)
        headerNames.diff(headerNames.distinct).distinct
      }

      row.zipWithIndex.map {
        case (value, index) ⇒
          if (value == null || value.isEmpty || value == options.nullValue) {
            // When there are empty strings or the values set in `nullValue`, put the
            // index as the suffix.
            s"_c$index"
          } else if (!caseSensitive && duplicates.contains(value.toLowerCase)) {
            // When there are case-insensitive duplicates, put the index as the suffix.
            s"$value$index"
          } else if (duplicates.contains(value)) {
            // When there are duplicates, put the index as the suffix.
            s"$value$index"
          } else {
            value
          }
      }
    } else {
      row.zipWithIndex.map {
        case (_, index) ⇒
          // Uses default column names, "_c#" where # is its position of fields
          // when header option is disabled.
          s"_c$index"
      }
    }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def tokenRdd(sparkSession: SparkSession,
               options: FixedWidthOptions,
               header: Array[String],
               inputPaths: Seq[String]): RDD[Array[String]] = {
    val rdd = baseRdd(sparkSession, options, inputPaths)
    // Make sure firstLine is materialized before sending to executors
    val firstLine = if (options.headerFlag) findFirstLine(options, rdd) else null
    tokenizer(rdd, firstLine, options)
  }

  def tokenizer(file: RDD[String], firstLine: String, params: FixedWidthOptions): RDD[Array[String]] = {
    // If header is set, make sure firstLine is materialized before sending to executors.
    val commentPrefix = params.comment.toString
    file.mapPartitions { iter ⇒
      val parser = new FixedWidthReader(params)
      val filteredIter = iter.filter { line ⇒
        line.trim.nonEmpty && !line.startsWith(commentPrefix)
      }
      if (params.headerFlag) {
        filteredIter.filterNot(_ == firstLine).map { item ⇒
          parser.parseLine(item.padTo(params.maxRowLength, params.paddingChar.toString).mkString("").substring(0, params.widths.sum))
        }
      } else {
        filteredIter.map { item ⇒
          parser.parseLine(item.padTo(params.maxRowLength, params.paddingChar.toString).mkString("").substring(0, params.widths.sum))
        }
      }
    }
  }
}
