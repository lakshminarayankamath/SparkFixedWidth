# FixedWidth Data Source for Apache Spark 2.1.1+

A library for parsing and querying FixedWidth data with Apache Spark.

## Requirements

This library requires: 
* Spark 2.1+
* Scala 2.11+

## Features
This package allows reading FixedWidth files in local or distributed filesystem as [Spark DataFrames](https://spark.apache.org/docs/2.1.0/sql-programming-guide.html).
When reading files the API accepts several options:
* `padding`: Padding character used to pad fields in fixed width files
* `encoding`: defaults to 'UTF-8' but can be set to other valid encoding names
* `widths`: stringified List of fixed width column lengths
* `comment`: skip lines beginning with this character. Default is `"\u0000"`
* `skipLines`: lines to skip from the beginning of the file
* `header`: when set to true the first line of files will be used to name columns and will not be included in data. All types will be assumed string. Default value is false.
* `inferSchema`: automatically infers column types. It requires one extra pass over the data and is false by default
* `recordSeparator`: end of line character. Default is `"\n"`
* `nullValue`: specifies a string that indicates a null value, any fields matching this string will be set as nulls in the DataFrame
* `dateFormat`: specifies a string that indicates the date format to use when reading dates or timestamps. Custom date formats follow the formats at [`java.text.SimpleDateFormat`](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html). This applies to both `DateType` and `TimestampType`. By default, it is `null` which means trying to parse times and date by `java.sql.Timestamp.valueOf()` and `java.sql.Date.valueOf()`.
* `maxCharsPerColumn`: Maximum number of characters allowed in a fixed width column. Default is 10000
* `maxRowLength`: Maximum number of characters allowed in a fixed width row. Default is 10000


Currently, write is not supported. This will be supported in future versions.

These examples use FixedWidth files available for download [here](https://github.com/lakshminarayankamath/SparkFixedWidth/tree/master/src/test/resources):

```
$ wget https://github.com/lakshminarayankamath/SparkFixedWidth/tree/master/src/test/resources
```
