package edu.berkeley.cs.succinct.sql

import edu.berkeley.cs.succinct.SuccinctCore
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

/**
 * Object that provides serialization/de-serialization methods for each tuple.
 */
class SuccinctSerDe(schema: StructType, separators: Array[Byte], limits: Seq[Int])
  extends Serializable {

  override def toString: String = {
    separators.map(_.toInt).mkString(",")
  }

  /**
   * Serializes a [[Row]] with input delimiters to create a single byte buffer.
   *
   * @param data The [[Row]] to be serialized.
   * @return The serialized [[Row]].
   */
  def serializeRow(data: Row): Array[Byte] = {
    assert(data.length == separators.length)
    assert(data.length == schema.length)
    separators.zip(Array.tabulate(data.length) { i => typeToString(i, data(i)).getBytes })
      .map(t => t._1 +: t._2)
      .flatMap(_.iterator)
  }

  def typeToString(elemIdx: Int, elem: Any): String = {
    if (elem == null) {
      "NULL"
    } else {
      schema(elemIdx).dataType match {
        case BooleanType => if (elem.asInstanceOf[Boolean]) "1" else "0"
        case ByteType => ("%0" + limits(elemIdx) + "d").format(elem.asInstanceOf[java.lang.Byte])
        case ShortType => ("%0" + limits(elemIdx) + "d").format(elem.asInstanceOf[java.lang.Short])
        case IntegerType => ("%0" + limits(elemIdx) + "d").format(elem.asInstanceOf[java.lang.Integer])
        case LongType => ("%0" + limits(elemIdx) + "d").format(elem.asInstanceOf[java.lang.Long])
        case FloatType => ("%0" + limits(elemIdx) + ".2f").format(elem.asInstanceOf[java.lang.Float])
        case DoubleType => ("%0" + limits(elemIdx) + ".2f").format(elem.asInstanceOf[java.lang.Double])
        case dType: DecimalType =>
          var digsBeforeDec = 0
          var digsAfterDec = 0
          if (dType.scale > 0) {
            digsAfterDec = dType.scale
          } else {
            digsAfterDec = dType.precision - dType.scale
          }
          digsBeforeDec = limits(elemIdx)
          val formatString = s"%0$digsBeforeDec.${digsAfterDec}f"
          formatString.format(elem.toString.toDouble)
        case StringType => elem.toString
        case other => throw new IllegalArgumentException(s"Unexpected type. ${schema(elemIdx).dataType}")
      }
    }
  }

  /**
   * De-serializes a single byte buffer to give back the original [[Row]].
   *
   * @param data The serialized [[Row]].
   * @return The de-serialized [[Row]].
   */
  def deserializeRow(data: Array[Byte]): Row = {
    val fieldTypes = schema.fields.map(_.dataType)
    var i = 0
    var elemList = new ListBuffer[String]
    val elemBuilder = new StringBuilder
    val separatorsIter = separators.iterator
    var nextSeparator = separatorsIter.next()
    while (i < data.length) {
      if (data(i) == nextSeparator) {
        if (i != 0) elemList += elemBuilder.toString
        elemBuilder.clear()
        if (separatorsIter.hasNext)
          nextSeparator = separatorsIter.next()
        else
          nextSeparator = SuccinctCore.EOL
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
   * @param requiredColumns Checks if a column is required or not.
   * @return The de-serialized [[Row]].
   */
  def deserializeRow(
      data: Array[Byte],
      requiredColumns: Map[String, Boolean]): Row = {
    if (data.length == 0 || !requiredColumns.values.reduce((a, b) => a | b)) return Row()
    val requiredFieldTypes = schema.fields.filter(field => requiredColumns(field.name)).map(_.dataType)
    val fieldNames = schema.fields.map(_.name)
    var i = 0
    var k = 0
    var elemList = new ListBuffer[String]
    val elemBuilder = new StringBuilder
    val separatorsIter = separators.iterator
    var nextSeparator: Byte = if (separatorsIter.hasNext) separatorsIter.next()
    else SuccinctCore.EOL
    while (i < data.length) {
      if (data(i) == nextSeparator) {
        if (i != 0 && requiredColumns(fieldNames(k - 1))) elemList += elemBuilder.toString
        elemBuilder.clear()
        nextSeparator = if (separatorsIter.hasNext) separatorsIter.next()
        else SuccinctCore.EOL
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
  def stringToType(elem: String, dataType: DataType): Any = {
    if (elem == "NULL") return null
    dataType match {
      case BooleanType => elem.equals("1")
      case ByteType => elem.toByte
      case ShortType => elem.toShort
      case IntegerType => elem.toInt
      case LongType => elem.toLong
      case FloatType => elem.toFloat
      case DoubleType => elem.toDouble
      case _: DecimalType => Decimal(new java.math.BigDecimal(elem))
      case StringType => elem
      case other => throw new IllegalArgumentException(s"Unexpected type $dataType.")
    }
  }

}
