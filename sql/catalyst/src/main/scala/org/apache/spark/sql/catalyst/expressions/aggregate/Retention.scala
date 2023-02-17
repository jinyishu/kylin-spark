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

package org.apache.spark.sql.catalyst.expressions.aggregate

import java.nio.ByteBuffer
import java.time.{DayOfWeek, Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalAdjusters}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.catalyst.expressions.objects.SerializerSupport
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * calculate event interval
 * @param startEventExpr start Event Expr
 * @param endEventExpr end Event Expr
 * @param eventTsExpr event timestamp
 * @param modelTypeLitExpr target event names
 *   SIMPLE , two different event
 *   SIMPLE_REL , two different event and relations
 *   REPEAT , two identical event,Include virtual events
 *   REPEAT_REL , two identical event ,Include virtual events and relations
 *   NOT_REPEAT_VIRTUAL , two identical event ,At least one virtual event
 *   NOT_REPEAT_VIRTUAL_REL,two identical event,At least one virtual event and relations
 * @param relationsExpr relations array
 * @param viewDimExpr view dimension
 * @param viewTypeLitExpr view dimension type
 *   ALL , whole
 *   START , start event
 *   END , end event
 *   USER , user tag
 *   USER_GROUP , user group
 * @param aggDateTypeLitExpr Aggregate time unit , ALL DAY WEEK MONTH
 * @param mutableAggBufferOffset
 * @param inputAggBufferOffset
 *    总体指标：是全局的概览，无法精确计算，因为每个第1天 第2天... 都是相对的日期，都是动态的日期，神策使用求平均值 或者 加权值
 *    也可以直接把所有的第一天用户求交集，然后与总人数求并集
 *    同时显示的指标：拿到所有的留存bitmap用户之后 单独发起一次查询 用bitmap过滤 ，用查看纬度分组，最后在拼接数据
 *    留存 / 流失： 留存为1 流失为-1
 */
case class Retention(startEventExpr: Expression, // 初始
                     endEventExpr: Expression, // 后续
                     eventTsExpr: Expression, // 时间戳
                     modelTypeLitExpr: Expression, // simple
                     relationsExpr: Expression, // 关联属性
                     viewDimExpr: Expression, // 按xx 查看 struct
                     aggDateTypeLitExpr: Expression, // 聚合日期时间类型
                     aggDateLengthLitExpr: Expression, // 聚合日期时间长度
                     startDateLitExpr: Expression, // 开始时间
                     endDateLitExpr: Expression, // 结束时间
                     mutableAggBufferOffset: Int = 0,
                     inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[mutable.HashMap[String, RetentionEvent]]
    with Serializable with Logging with SerializerSupport {

  def this(startEventExpr: Expression, endEventExpr: Expression,
           eventTsExpr: Expression,
           modelTypeLitExpr: Expression, relationsExpr: Expression,
           viewDimExpr: Expression, aggDateTypeLitExpr: Expression,
           aggDateLengthLitExpr: Expression,
           startDateLitExpr: Expression, endDateLitExpr: Expression) = {
    this(startEventExpr, endEventExpr, eventTsExpr, modelTypeLitExpr,
      relationsExpr, viewDimExpr, aggDateTypeLitExpr, aggDateLengthLitExpr,
      startDateLitExpr, endDateLitExpr, 0, 0)
  }

  val kryo: Boolean = true


  lazy val modelType: String = modelTypeLitExpr.eval().toString

  val SIMPLE = "SIMPLE"
  val SIMPLE_REL = "SIMPLE_REL"
  val REPEAT = "REPEAT"
  val REPEAT_REL = "REPEAT_REL"
  val NOT_REPEAT_VIRTUAL = "NOT_REPEAT_VIRTUAL"
  val NOT_REPEAT_VIRTUAL_ERL = "NOT_REPEAT_VIRTUAL_REL"


  lazy val aggDateType: String = aggDateTypeLitExpr.eval().toString
  val DATE_DAY = "DAY"
  val DATE_WEEK = "WEEK"
  val DATE_MONTH = "MONTH"
  lazy val aggDateLength: Int = aggDateLengthLitExpr.eval().toString.toInt + 1
  lazy val startDate: String = startDateLitExpr.eval().toString
  lazy val endDate: String = endDateLitExpr.eval().toString

  lazy val dayFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  lazy val monthFormatter = DateTimeFormatter.ofPattern("yyyy-MM")

  lazy val endLocalDateDay: LocalDate = LocalDate.parse(endDate, dayFormatter)
  lazy val endLocalDateMonth: LocalDate = LocalDate.parse(endDate, monthFormatter)

  val VIEW_ALL = "ALL"
  val VIEW_START = "START"
  val VIEW_END = "END"
  val VIEW_USER = "USER"
  val VIEW_USER_GROUP = "USER_GROUP"


  var dataTypeValue: DataType = null


  override def createAggregationBuffer(): mutable.HashMap[String, RetentionEvent] =
    mutable.HashMap[String, RetentionEvent]()

  def doAggDate(ts: Long): String = {
    val localDate = LocalDateTime.ofInstant(
      Instant.ofEpochMilli(ts), ZoneId.systemDefault
    ).toLocalDate
    aggDateType match {
      case DATE_DAY =>
        localDate.format(dayFormatter)
      case DATE_WEEK =>
        localDate.`with`(DayOfWeek.MONDAY).format(dayFormatter)
      case DATE_MONTH =>
        localDate.`with`(TemporalAdjusters.firstDayOfMonth).format(monthFormatter)
    }
  }

  override def update(buffer: mutable.HashMap[String, RetentionEvent],
                      input: InternalRow): mutable.HashMap[String, RetentionEvent] = {
    val ts = evalToLong(eventTsExpr, input)
    val eventType = startEventExpr.eval(input).toString.toInt
    val aggDate = doAggDate(ts)
    val key = eventType + aggDate
    if(!buffer.contains(key)) {
      val event = RetentionEvent(ts, eventType, aggDate, null)
      buffer(key) = event
    }
    buffer
  }



  override def merge(buffer: mutable.HashMap[String, RetentionEvent],
                     input: mutable.HashMap[String, RetentionEvent]
                    ): mutable.HashMap[String, RetentionEvent] = {
    // todo 有纬度查看的时候，按时间合并选择最早的起始结束事件
    buffer ++= input
  }



  override def eval(buffer: mutable.HashMap[String, RetentionEvent]): GenericArrayData = {


    val result = ListBuffer[RetentionResult]()
    val startLocalDate: LocalDate = LocalDate.parse(startDate, dayFormatter)
    for (i <- (0 until(aggDateLength))) {
      val plusDate = plusDays(startLocalDate, i)
      if(plusDate.until(endLocalDateDay, ChronoUnit.DAYS) >= 0) {
        val plusDateFormat = plusDate.format(dayFormatter)
        val startEventKey = RetentionEventType.START + plusDateFormat
        val startEvent = buffer.getOrElse(startEventKey, null)
        if (startEvent != null) {
          val arr = new Array[Int](aggDateLength)
          for (j <- (0 until(aggDateLength))) {
            val endEventPlusDate = plusDays(startLocalDate, i + j)
            val endEventKey = RetentionEventType.END + endEventPlusDate.format(dayFormatter)
            val endEvent = buffer.getOrElse(endEventKey, null)
            if(endEvent == null) {
              arr(j) = -1
            } else {
              arr(j) = 1
            }
          }
          result.append(RetentionResult(plusDateFormat, arr))
        }
      }
    }
    new GenericArrayData(result.map(e => {
      InternalRow(
        UTF8String.fromString(e.aggDate),
        new GenericArrayData(e.result)
      )
    }))

  }

  def plusDays(startDate: LocalDate, i: Int): LocalDate = {
    aggDateType match {
      case DATE_DAY => startDate.plusDays(i)
      case DATE_WEEK => startDate.plusWeeks(i)
      case DATE_MONTH => startDate.plusMonths(i)
    }
  }


  override def dataType: DataType = {
    if (dataTypeValue != null) {
      return dataTypeValue
    }
    dataTypeValue = ArrayType(
      StructType(Seq(
        StructField("agg_date", StringType),
        StructField("res", ArrayType(IntegerType))
      )))
    dataTypeValue
  }
  override def serialize(buffer: mutable.HashMap[String, RetentionEvent]): Array[Byte] = {
    serializerInstance.serialize(buffer).array()
  }

  override def deserialize(storageFormat: Array[Byte]): mutable.HashMap[String, RetentionEvent] = {
    serializerInstance.deserialize(ByteBuffer.wrap(storageFormat))
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = false

  override def children: Seq[Expression] =
    startEventExpr :: endEventExpr :: eventTsExpr :: modelTypeLitExpr ::
      relationsExpr :: viewDimExpr :: aggDateTypeLitExpr :: aggDateLengthLitExpr ::
      startDateLitExpr :: endDateLitExpr :: Nil


  def evalToLong(expr: Expression, input: InternalRow): Long = {
    expr.dataType match {
      case _: NumericType =>
        expr.eval(input).toString.toLong
      case _: TimestampType =>
        expr.eval(input).toString.toLong / 1000
      case _: NullType =>
        -1L
      case _ =>
        // timezone doesn't really matter here
        val tsColumn = Cast(expr, TimestampType, Some("UTC")).eval(input)
        if (tsColumn == null) {
          return expr.eval(input).toString.toLong
        }
        tsColumn.toString.toLong / 1000
    }
  }


  def evalToString(expr: Expression, input: InternalRow): String = {
    val raw = expr.eval(input)
    if (raw != null) {
      raw.toString
    } else {
      null
    }
  }

  def evalToArray(expr: Expression, input: InternalRow): Array[String] = {
    Option(expr.eval(input)) match {
      case Some(arr: GenericArrayData) =>
        arr.asInstanceOf[GenericArrayData].array.map(e => {
          if ( e != null ) e.toString else null
        })
      case _ =>
        null
    }
  }

}
// scalastyle:off
case class RetentionEvent(ts: Long,
                         eventType: Int = -1,
                         aggDateType: String ,
                         var groupingInfo: String
                        ) {
  def typeOrdering: Int = eventType

  // override hashcode otherwise changes in contrib field will affect the hashCode
  override def hashCode(): Int = System.identityHashCode(this)

  override def equals(obj: scala.Any) = obj.hashCode() == this.hashCode()
}
case class RetentionResult(aggDate: String , result: Array[Int])

object RetentionEventType {
  val NULL: Int = -1
  val START: Int = 0
  val END: Int = 1
  val REPEAT: Int = 2
}