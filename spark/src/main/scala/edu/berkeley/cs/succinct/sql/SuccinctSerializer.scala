package edu.berkeley.cs.succinct.sql

import edu.berkeley.cs.succinct.SuccinctIndexedBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

/**
 * Object that provides serialization/de-serialization methods for each tuple.
 */
object SuccinctSerializer {

  /**
   * Serializes a [[Row]] with input delimiters to create a single byte buffer.
   *
   * @param data The [[Row]] to be serialized.
   * @param separators The list of separators.
   * @return The serialized [[Row]].
   */
  private[succinct] def serializeRow(data: Row, separators: Array[Byte], schema: StructType): Array[Byte] = {
    assert(data.length == separators.length)
    assert(data.length == schema.length)
    val dataMap = data.toSeq.zip(schema.fields)
    separators.zip(dataMap.map(t => typeToString(t._1, t._2.dataType).getBytes))
      .map(t => t._1 +: t._2)
      .flatMap(_.iterator)
  }

  /**
   * De-serializes a single byte buffer to give back the original [[Row]].
   *
   * @param data The serialized [[Row]].
   * @param separators The list of separators.
   * @param schema The schema for the [[Row]].
   * @return The de-serialized [[Row]].
   */
  private[succinct] def deserializeRow(
      data: Array[Byte],
      separators: Array[Byte],
      schema: StructType): Row = {
    val fieldTypes = schema.fields.map(_.dataType)
    var i = 0
    var elemList = new ListBuffer[String]
    val elemBuilder = new StringBuilder
    val separatorsIter = separators.iterator
    var nextSeparator = separatorsIter.next
    while (i < data.length) {
      if (data(i) == nextSeparator) {
        if (i != 0) elemList += elemBuilder.toString
        elemBuilder.clear
        if (separatorsIter.hasNext)
          nextSeparator = separatorsIter.next
        else
          nextSeparator = SuccinctIndexedBuffer.getRecordDelim
      } else {
        elemBuilder.append(data(i).toChar)
      }
      i += 1
    }
    elemList += elemBuilder.toString
    assert(separators.length == elemList.length)
    assert(elemList.length == fieldTypes.length)
    Row.fromSeq(elemList.zip(fieldTypes).map(t => stringToType(t._1, t._2)).toSeq)
  }

  /**
   * De-serializes a single byte buffer to give back the [[Row]] with pruned columns.
   *
   * @param data The serialized [[Row]].
   * @param separators The list of separators.
   * @param schema The schema for the [[Row]].
   * @param requiredColumns Checks if a column is required or not.
   * @return The de-serialized [[Row]].
   */
  private[succinct] def deserializeRow(
      data: Array[Byte],
      separators: Array[Byte],
      schema: StructType,
      requiredColumns: Map[String, Boolean]): Row = {
    val requiredFieldTypes = schema.fields.filter(field => requiredColumns(field.name)).map(_.dataType)
    val fieldNames = schema.fields.map(_.name)
    var i = 0
    var k = 0
    var elemList = new ListBuffer[String]
    val elemBuilder = new StringBuilder
    val separatorsIter = separators.iterator
    var nextSeparator = separatorsIter.next
    while (i < data.length) {
      if (data(i) == nextSeparator) {
        if (i != 0 && requiredColumns(fieldNames(k - 1))) elemList += elemBuilder.toString
        elemBuilder.clear
        if (separatorsIter.hasNext)
          nextSeparator = separatorsIter.next
        else
          nextSeparator = SuccinctIndexedBuffer.getRecordDelim
        k += 1
      } else {
        elemBuilder.append(data(i).toChar)
      }
      i += 1
    }
    if (requiredColumns(fieldNames(k - 1))) elemList += elemBuilder.toString
    Row.fromSeq(elemList.zip(requiredFieldTypes).map(t => stringToType(t._1, t._2)).toSeq)
  }

  /**
   * Converts a String to the corresponding data-type based on the input data type.
   *
   * @param elem The string element to be converted.
   * @param dataType The data-type of the element.
   * @return The type-converted data.
   */
  private[succinct] def stringToType(elem: String, dataType: DataType): Any = {
    if(elem == "NULL") return null
    dataType match {
      case BooleanType => elem.equals("1")
      case ByteType => elem.toByte
      case ShortType => elem.toShort
      case IntegerType => elem.toInt
      case LongType => elem.toLong
      case FloatType => elem.toFloat
      case DoubleType => elem.toDouble
      case _:DecimalType => new java.math.BigDecimal(elem)
      case StringType => elem
      case other => throw new IllegalArgumentException(s"Unexpected type $dataType.")
    }
  }

  private[succinct] def typeToString(elem: Any, dataType: DataType): String = {
    if(elem == null) return "NULL"
    dataType match {
      case BooleanType => if(elem.asInstanceOf[Boolean]) "1" else "0"
      case ByteType => "%03d".format(elem.asInstanceOf[java.lang.Byte])
      case ShortType => "%05d".format(elem.asInstanceOf[java.lang.Short])
      case IntegerType => "%010d".format(elem.asInstanceOf[java.lang.Integer])
      case LongType => "%019d".format(elem.asInstanceOf[java.lang.Long])
      case FloatType => "%014.3f".format(elem.asInstanceOf[java.lang.Float])
      case DoubleType => "%014.3f".format(elem.asInstanceOf[java.lang.Double])
      case dType:DecimalType => {
        var digsBeforeDec = 0
        var digsAfterDec = 0
        if(dType.scale > 0) {
          digsBeforeDec = dType.precision - dType.scale
          digsAfterDec = dType.scale
        } else {
          digsBeforeDec = 1
          digsAfterDec = dType.precision - dType.scale
        }
        val formatString = s"%0${digsBeforeDec}.${digsAfterDec}f"
        formatString.format(elem.asInstanceOf[java.math.BigDecimal].toString.toDouble)
      }
      case StringType => elem.asInstanceOf[String]
      case other => throw new IllegalArgumentException(s"Unexpected type. $dataType")
    }
  }

}
