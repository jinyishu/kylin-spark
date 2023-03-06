/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions


import java.io.{ByteArrayInputStream, ByteArrayOutputStream, Closeable, DataInputStream, DataOutputStream}
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.roaringbitmap.longlong.Roaring64NavigableMap

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, DataType, NumericType, StringType}
import org.apache.spark.unsafe.types.UTF8String


/**
 * CompressBitmapContains
 */
case class CompressBitmapContains(column: Expression, encodeBitmap: Expression)
  extends BinaryExpression with ExpectsInputTypes{

  override def left: Expression = column
  override def right: Expression = encodeBitmap

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, StringType)

  lazy val bitmap: Roaring64NavigableMap = {
    val literalValue = right.asInstanceOf[Literal].value
    val utf8Bitmap = literalValue.asInstanceOf[UTF8String].toString
    try {
      if (utf8Bitmap.startsWith("gzip:")) { // gzip compress
        CompressBitmapUtils.compressEncoderStringToBitmap(utf8Bitmap)
      } else {
        val bitmapByte: Array[Byte] = java.util.Base64.getDecoder.decode(utf8Bitmap)
        CompressBitmapUtils.serializeBytesToBitmap(bitmapByte)
      }
    } catch {
      case _: Throwable =>
        val bitmapByte: Array[Byte] = java.util.Base64.getDecoder.decode(utf8Bitmap)
        CompressBitmapUtils.serializeBytesToBitmap(bitmapByte)
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    val bitmapRef = ctx.addReferenceObj("compressbitmap", bitmap)
    val compressBitmapUtils = CompressBitmapUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (left, right) => {
      s"""$compressBitmapUtils.bitmapContainsImpl($left, $bitmapRef)"""
    })
  }

  override protected def nullSafeEval(column: Any, encodeBitmap: Any): Any = {
    if (bitmap.isEmpty) {
      return false
    }
    CompressBitmapUtils.bitmapContainsImpl(column, bitmap)
  }

  override def dataType: DataType = BooleanType

  override def prettyName: String = "compress_bitmap_contains"

}
/**
 * CompressGetBitmap
 */
case class CompressGetBitmap(driver: Expression, url: Expression,
                             username: Expression, passwd: Expression,
                             querySql: Expression)
  extends SeptenaryExpression
    with ExpectsInputTypes{

  override def children: Seq[Expression] = Seq(
    driver, url, username, passwd, querySql
  )
  override def inputTypes: Seq[AbstractDataType] = Seq(
    StringType, StringType, StringType, StringType, StringType
  )

  override def dataType: StringType = StringType

  override def eval(input: InternalRow): Any = {
    val exprs = children
    val v1 = exprs(0).eval(input)
    val v2 = exprs(1).eval(input)
    val v3 = exprs(2).eval(input)
    val v4 = exprs(3).eval(input)
    val v5 = exprs(4).eval(input)
    if (v1 != null && v2 != null && v3 != null && v4 != null && v5 != null) {
      return nullSafeEval(v1, v2, v3, v4, v5, None, None)
    }
    null
  }
  override protected def nullSafeEval(
           input1: Any, input2: Any, input3: Any, input4: Any,
           input5: Any, input6: Any, input7: Option[Any]
                                     ): Any = {
    CompressBitmapUtils.getBitmapString(
      input1.toString, input2.toString, input3.toString,
      input4.toString, input5.toString
    )
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sb = CompressBitmapUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (arg1, arg2, arg3, arg4, arg5, arg6, arg7) => {
      s"""$sb.getBitmapString($arg1,$arg2,$arg3,$arg4,$arg5)"""
    })
  }
  override def prettyName: String = "compress_get_bitmap"
}
/**
 * CompressBitmapUtils
 */
object CompressBitmapUtils{

  def serializeBitmapToBytes(bitmap: Roaring64NavigableMap): Array[Byte] = {
    bitmap.runOptimize()
    var bos: ByteArrayOutputStream = null
    var dos: DataOutputStream = null
    try {
      bos = new ByteArrayOutputStream()
      dos = new DataOutputStream(bos)
      bitmap.serialize(dos)
    } catch {
      case _: Throwable => throw new Exception()
    } finally {
      closeQuiet(dos)
      closeQuiet(bos)
    }
    bos.toByteArray
  }

  def serializeBytesToBitmap(b: Array[Byte]): Roaring64NavigableMap = {
    val bitmap: Roaring64NavigableMap = new Roaring64NavigableMap()
    var bin: ByteArrayInputStream = null
    var dis: DataInputStream = null
    try {
      bin = new ByteArrayInputStream(b)
      dis = new DataInputStream(bin)
      bitmap.deserialize(dis)
    } catch {
      case _: Throwable => throw new Exception()
    } finally {
      closeQuiet(dis)
      closeQuiet(bin)
    }
    bitmap
  }

  def compressByte(src: Array[Byte]): Array[Byte] = {
    if (src == null || src.length == 0) return Array.empty[Byte]
    var bos: ByteArrayOutputStream = null
    var gzip: GZIPOutputStream = null
    try {
      bos = new ByteArrayOutputStream()
      gzip = new GZIPOutputStream(bos)
      gzip.write(src)
    } catch {
      case _: Throwable => throw new Exception()
    } finally {
      closeQuiet(gzip)
      closeQuiet(bos)
    }
    bos.toByteArray
  }

  def decompressByte(compress: Array[Byte]): Array[Byte] = {
    if (compress == null || compress.length == 0) return Array.empty[Byte]
    var bos: ByteArrayOutputStream = null
    var bis: ByteArrayInputStream = null
    var gzip: GZIPInputStream = null
    try {
      bos = new ByteArrayOutputStream()
      bis = new ByteArrayInputStream(compress)
      gzip = new GZIPInputStream(bis)
      val buffer: Array[Byte] = new Array[Byte](256)
      var n = gzip.read(buffer)
      while (n >= 0) {
        bos.write(buffer, 0, n)
        n = gzip.read(buffer)
      }
    } catch {
      case _: Throwable => throw new Exception()
    } finally {
      closeQuiet(gzip)
      closeQuiet(bis)
      closeQuiet(bos)
    }
    bos.toByteArray
  }

  def bitmapToCompressEncoderString(bitmap: Roaring64NavigableMap): String = {
    val bitmap2byte = serializeBitmapToBytes(bitmap)
    val compressed = compressByte(bitmap2byte)
    "gzip:" + java.util.Base64.getEncoder.encodeToString(compressed)
  }

  def compressEncoderStringToBitmap(src: String): Roaring64NavigableMap = {
    val compressByte: Array[Byte] = java.util.Base64.getDecoder.decode(src.substring(5))
    val decompressStringByte = decompressByte(compressByte)
    serializeBytesToBitmap(decompressStringByte)
  }

  def closeQuiet(c: Closeable): Unit =
    if (c != null) try {
      c.close()
    } catch {
      case _: Throwable =>
    }


  def bitmapContainsImpl(column: Any, bitmapArg: Roaring64NavigableMap): Boolean = {
    column match {
      case value: Integer =>
        bitmapArg.contains(value.asInstanceOf[Integer].longValue())
      case value: Long =>
        bitmapArg.contains(value.asInstanceOf[Long])
      case _ => false
    }
  }

  def getBitmapString(
                       driver: String, url: String, username: String,
                       passwd: String, querySql: String
                     ): String = {
    var connection: Connection = null
    var value = ""
    try {
      if (connection == null) {
        // scalastyle:off classforname
        Class.forName(driver)
        // scalastyle:on classforname
        connection = DriverManager.getConnection(url, username, passwd)
      }
      val statement = connection.createStatement()
      val res: ResultSet = statement.executeQuery(querySql)
      while (res.next) {
        value = res.getString(1)
      }
    } catch {
      case e: Exception => e.printStackTrace().toString
    } finally {
      connection.close()
    }
    value
  }

}