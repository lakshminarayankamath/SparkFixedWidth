package org.apache.spark.sql.contrib.format.fixedwidth

import java.sql.Timestamp
import java.text.SimpleDateFormat

import scala.util.Try

import org.apache.commons.lang.time.FastDateFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

/**
  * The class provides methods to to return the schema of the file to the calling function
  */
class FixedWidthInferSchema extends Serializable {
  def infer(tokenRdd: RDD[Array[String]], header: Array[String], options: FixedWidthOptions): StructType = {
    val transposedRDD = tokenRdd.take(100).transpose
    val res = FastDateFormat.getInstance("yyyy-MM-dd")
    val dataTypes = transposedRDD.map(col => col.map(field => inferField(StringType, field, options.nullValue, options.dateFormat)))
    val columnTypes = dataTypes.map(col => col.groupBy(identity).maxBy(_._2.size)._1)

    val structFields = header.zip(columnTypes).map { case (thisHeader, rootType) =>
      val dType = rootType match {
        case z: NullType => StringType
        case other => other
      }
      StructField(thisHeader, dType, nullable = true)
    }

    StructType(structFields)
  }

  /**
    * Infer type of string field. Given known type Double, and a string "1", there is no
    * point checking if it is an Int, as the final type must be Double or higher.
    */
  private def inferField(typeSoFar: DataType,
                              field: String,
                              nullValue: String = "",
                              dateFormatter: SimpleDateFormat = null): DataType = {
    def tryParseInteger(field: String): DataType = if (Try(field.toInt).isSuccess) {
      IntegerType
    } else {
      tryParseLong(field)
    }

    def tryParseLong(field: String): DataType = if (Try(field.toLong).isSuccess) {
      LongType
    } else {
      tryParseDouble(field)
    }

    def tryParseDouble(field: String): DataType = {
      if (Try(field.toDouble).isSuccess) {
        DoubleType
      } else {
        tryParseTimestamp(field)
      }
    }

    def tryParseTimestamp(field: String): DataType = {
      if (dateFormatter != null) {
        // This case infers a custom `dataFormat` is set.
        if (Try(dateFormatter.parse(field)).isSuccess){
          TimestampType
        } else {
          tryParseBoolean(field)
        }
      } else {
        // We keep this for backward compatibility.
        if (Try(Timestamp.valueOf(field)).isSuccess) {
          TimestampType
        } else {
          tryParseBoolean(field)
        }
      }
    }

    def tryParseBoolean(field: String): DataType = {
      if (Try(field.toBoolean).isSuccess) {
        BooleanType
      } else {
        stringType()
      }
    }

    // Defining a function to return the StringType constant is necessary in order to work around
    // a Scala compiler issue which leads to runtime incompatibilities with certain Spark versions;
    // see issue #128 for more details.
    def stringType(): DataType = {
      StringType
    }

    if (field == null || field.isEmpty || field == nullValue) {
      typeSoFar
    } else {
      tryParseInteger(field)
    }
  }

  /**
    * Copied from internal Spark api
    * [[org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion]]
    */
  private val numericPrecedence: IndexedSeq[DataType] =
    IndexedSeq[DataType](
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      TimestampType,
      DecimalType.DoubleDecimal)

  /**
    * Copied from internal Spark api
    * [[org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion]]
    */
  val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (NullType, t1) => Some(t1)
    case (t1, NullType) => Some(t1)
    case (StringType, t2) => Some(StringType)
    case (t1, StringType) => Some(StringType)

    // Promote numeric types to the highest of the two and all numeric types to unlimited decimal
    case (t1, t2) if Seq(t1, t2).forall(numericPrecedence.contains) =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    case _ => None
  }
}
