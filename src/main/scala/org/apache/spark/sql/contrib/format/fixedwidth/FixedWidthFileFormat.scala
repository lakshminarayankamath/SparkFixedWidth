package org.apache.spark.sql.contrib.format.fixedwidth

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce._
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.{HadoopFileLinesReader, OutputWriterFactory, PartitionedFile, TextBasedFileFormat}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

/**
  * This class is the base class for implementing Fixed Width format as a new datasource
  */
class FixedWidthFileFormat extends TextBasedFileFormat with DataSourceRegister {

  /**
    * The string that represents the format that this data source provider uses. This is
    * overridden by children to provide a nice alias for the data source
    */
  override def shortName(): String = "fixed"

  override def toString: String = "FIXED"

  /**
    * This function infers the schema of the fixed width file
    */
  override def inferSchema(sparkSession: SparkSession,
                           options: Map[String, String],
                           files: Seq[FileStatus]): Option[StructType] = {
    val fixedWidthUtils   = new FixedWidthUtils()
    val fixedWidthOptions = new FixedWidthOptions(options)

    val paths         = files.filterNot(_.getPath.getName startsWith "_").map(_.getPath.toString)
    val rdd           = fixedWidthUtils.baseRdd(sparkSession, fixedWidthOptions, paths)
    val firstLine     = fixedWidthUtils.findFirstLine(fixedWidthOptions, rdd)
    val firstRow      = new FixedWidthReader(fixedWidthOptions).parseLine(firstLine)
    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    val header        = fixedWidthUtils.makeSafeHeader(firstRow, fixedWidthOptions, caseSensitive)

    val parsedRdd = fixedWidthUtils.tokenRdd(sparkSession, fixedWidthOptions, header, paths)
    val schema = if (fixedWidthOptions.inferSchemaFlag) {
      new FixedWidthInferSchema().infer(parsedRdd, header, fixedWidthOptions)
    } else {
      // By default fields are assumed to be StringType
      val schemaFields = header.map { fieldName ⇒
        StructField(fieldName, StringType, nullable = true)
      }
      StructType(schemaFields)
    }
    Some(schema)
  }

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = ???

  /**
    * Returns a function that can be used to read a single file in as an Iterator of InternalRow.
    *
    * @param dataSchema The global data schema. It can be either specified by the user, or
    *                   reconciled/merged from all underlying data files. If any partition columns
    *                   are contained in the files, they are preserved in this schema.
    * @param partitionSchema The schema of the partition column row that will be present in each
    *                        PartitionedFile. These columns should be appended to the rows that
    *                        are produced by the iterator.
    * @param requiredSchema The schema of the data that should be output for each row.  This may be a
    *                       subset of the columns that are present in the file if column pruning has
    *                       occurred.
    * @param filters A set of filters than can optionally be used to reduce the number of rows output
    * @param options A set of string -> string configuration options.
    * @return
    */
  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  override def buildReader(sparkSession: SparkSession,
                           dataSchema: StructType,
                           partitionSchema: StructType,
                           requiredSchema: StructType,
                           filters: Seq[Filter],
                           options: Map[String, String],
                           hadoopConf: Configuration): (PartitionedFile) ⇒ Iterator[InternalRow] = {
    val fixedWidthOptions = new FixedWidthOptions(options)

    // Broadcast a hadoofConf variable to the cluster
    val broadcastedHadoopConf = {
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    }

    // Function that can be used to read a single file in as an Iterator of InternalRow.
    (file: PartitionedFile) ⇒
      {
        val lineIterator = {
          val conf        = broadcastedHadoopConf.value.value
          val linesReader = new HadoopFileLinesReader(file, conf)
          Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ ⇒ linesReader.close()))
          linesReader.map { line ⇒
            new String(line.getBytes, 0, line.getLength, fixedWidthOptions.encoding)
          }
        }

        // Drop lines with comments and empty lines
        val nonEmptyLines = if (fixedWidthOptions.isCommentSet) {
          val commentPrefix = fixedWidthOptions.comment.toString
          lineIterator.filterNot { line ⇒
            line.trim.isEmpty || line.trim.startsWith(commentPrefix)
          }
        } else {
          lineIterator.filterNot(_.trim.isEmpty)
        }
        // Drop header line (if present)
        if (fixedWidthOptions.headerFlag && file.start == 0) {
          if (nonEmptyLines.hasNext) {
            nonEmptyLines.drop(1)
          }
        }
        // If skipLines is present, drop the specified number of lines
        if (fixedWidthOptions.skipLines > 0) {
          nonEmptyLines.drop(fixedWidthOptions.skipLines)
        }
        val maxRowLength = fixedWidthOptions.maxRowLength
        val internalRow = nonEmptyLines.map(i ⇒ {
          val fixedWidthParser = new FixedWidthReader(fixedWidthOptions)
          // Get a row represented as Array[Strings]
          val rowStrings = fixedWidthParser.parseLine(
            // Pad rows with length < maxRowLength and trim rows with length > maxRowLength
            i.padTo(maxRowLength, fixedWidthOptions.paddingChar.toString).mkString("").substring(0, maxRowLength)
          )
          // Convert to UTF-8 strings
          val utf8Strings = rowStrings.map(x ⇒ UTF8String.fromString(x))
          // Create generic internal row with the required number of columns
          val row = new GenericInternalRow(utf8Strings.length)
          // Update column for each row
          utf8Strings.toList.zipWithIndex.foreach { case (element, index) ⇒ row.update(index, element) }
          row
        })
        internalRow
      }
  }
}
