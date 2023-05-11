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
import java.time.{DayOfWeek, Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAdjusters
import java.util.Locale

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

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
 */
case class Interval(startEventExpr: Expression,
                    endEventExpr: Expression,
                    eventTsExpr: Expression,
                    modelTypeLitExpr: Expression,
                    relationsExpr: Expression,
                    viewDimExpr: Expression,
                    viewTypeLitExpr: Expression,
                    aggDateTypeLitExpr: Expression,
                    mutableAggBufferOffset: Int = 0,
                    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ListBuffer[IntervalEvent]]
    with Serializable with Logging with SerializerSupport {

  def this(startEventExpr: Expression,
           endEventExpr: Expression,
           eventTsExpr: Expression,
           modelTypeLitExpr: Expression,
           relationsExpr: Expression,
           viewDimExpr: Expression,
           viewTypeLitExpr: Expression,
           aggDateTypeLitExpr: Expression) = {
    this(startEventExpr, endEventExpr, eventTsExpr, modelTypeLitExpr,
      relationsExpr, viewDimExpr, viewTypeLitExpr, aggDateTypeLitExpr, 0, 0)
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

  val DATE_ALL = "ALL"
  val DATE_DAY = "DAY"
  val DATE_WEEK = "WEEK"
  val DATE_MONTH = "MONTH"

  lazy val dayFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.CHINESE)
  lazy val monthFormatter = DateTimeFormatter.ofPattern("yyyy-MM", Locale.CHINESE)

  lazy val viewType: String = viewTypeLitExpr.eval().toString

  val VIEW_ALL = "ALL"
  val VIEW_START = "START"
  val VIEW_END = "END"
  val VIEW_USER = "USER"
  val VIEW_USER_GROUP = "USER_GROUP"




  var dataTypeValue: DataType = null

  override def createAggregationBuffer(): ListBuffer[IntervalEvent] = ListBuffer[IntervalEvent]()

  override def update(buffer: ListBuffer[IntervalEvent],
                      input: InternalRow): ListBuffer[IntervalEvent] = {
    try {
      val ts = evalToLong(eventTsExpr, input)
      if (ts < 0) return buffer
      val eventType = modelType match {
        case SIMPLE | SIMPLE_REL | REPEAT | REPEAT_REL =>
          // simple : 0 1 -1
          // repeat: 2 -1
          startEventExpr.eval(input).toString.toInt
        case NOT_REPEAT_VIRTUAL | NOT_REPEAT_VIRTUAL_ERL =>
          // 0 1 2 -1
          val startEventType: Int = startEventExpr.eval(input).toString.toInt
          val endEventType: Int = endEventExpr.eval(input).toString.toInt
          if (startEventType == EventType.NULL && endEventType == EventType.NULL) {
            EventType.NULL
          } else if (startEventType == EventType.START && endEventType == EventType.END) {
            EventType.REPEAT
          } else {
            startEventType.max(endEventType)
          }
        case _ => EventType.NULL
      }
      if (eventType == EventType.NULL) return buffer
      val aggDate = doAggDate(ts)
      val groupingInfo = evalToString(viewDimExpr, input)
      val relatedDims = evalToArray(relationsExpr, input)
      val event = IntervalEvent(ts, eventType, aggDate, relatedDims, groupingInfo)
      buffer.append(event)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    buffer
  }

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
      case _ => DATE_ALL
    }
  }

  override def merge(buffer: ListBuffer[IntervalEvent],
                     input: ListBuffer[IntervalEvent]): ListBuffer[IntervalEvent] = {
    buffer ++= input
  }

  def doSimpleEval(sorted: ListBuffer[IntervalEvent],
                   result: ListBuffer[IntervalEvent]): Unit = {
    var firstStep: IntervalEvent = null
    for (event <- sorted) {
      // start -> end  start -> end
      event.eventType match {
        case EventType.START =>
          if (firstStep == null) firstStep = event
        case EventType.END if firstStep != null =>
          firstStep.interval = event.ts - firstStep.ts
          if (viewType == VIEW_END) firstStep.groupingInfo = event.groupingInfo
          result.append(firstStep)
          firstStep = null
        case _ =>
      }
    }
  }

  def doSimpleRelEval(sorted: ListBuffer[IntervalEvent],
                      result: ListBuffer[IntervalEvent]): Unit = {
    val map: mutable.HashMap[String, IntervalEvent] = mutable.HashMap[String, IntervalEvent]()
    for (event <- sorted) {
      // start -> rel -> end
      event.eventType match {
        case EventType.START =>
          val startRel = event.relatedDims.apply(0)
          if (startRel != null && !map.contains(startRel)) {
            map(startRel) = event
          }
        case EventType.END =>
          val endRel = event.relatedDims.apply(1)
          if (endRel != null) {
            val startEvent = map.getOrElse(endRel, null)
            if (startEvent != null) {
              startEvent.interval = event.ts - startEvent.ts
              if (viewType == VIEW_END) startEvent.groupingInfo = event.groupingInfo
              result.append(startEvent)
              map -= endRel
            }
          }
        case _ =>
      }
    }
  }

  def doRepeatEval(sorted: ListBuffer[IntervalEvent],
                   result: ListBuffer[IntervalEvent]): Unit = {
    var firstStep: IntervalEvent = null
    for (event <- sorted) {
      // event.eventType 2  repeat -> repeat
      if (firstStep == null) {
        firstStep = event
      } else {
        firstStep.interval = event.ts - firstStep.ts
        if (viewType == VIEW_END) firstStep.groupingInfo = event.groupingInfo
        result.append(firstStep)
        firstStep = null
      }
    }
  }
  def doRepeatRelEval(sorted: ListBuffer[IntervalEvent],
                      result: ListBuffer[IntervalEvent]): Unit = {
    val map: mutable.HashMap[String, IntervalEvent] = mutable.HashMap[String, IntervalEvent]()

    for (event <- sorted) {
      // 2 -> rel -> 2
      breakable {
        val startRel = event.relatedDims.apply(0)
        val endRel = event.relatedDims.apply(1)
        if (startRel != null && map.isEmpty) {
          // The map is empty, and the first is start
          map(startRel) = event
          break()
        }
        if (map.nonEmpty) {
          // Judge whether the end has a match, calculate if it has,
          // and store start if it has not
          if (endRel != null) {
            val startEvent = map.getOrElse(endRel, null)
            if (startEvent != null) {
              // Match to end, remove start after calculation
              startEvent.interval = event.ts - startEvent.ts
              if (viewType == VIEW_END) startEvent.groupingInfo = event.groupingInfo
              result.append(startEvent)
              map -= endRel
              break()
            }
          }
          if (startRel != null && !map.contains(startRel)) {
            // End does not match, storage start
            map(startRel) = event
          }
        }
      }
    }
  }

  def doNotRepeatVirtualEval(sorted: ListBuffer[IntervalEvent],
                             result: ListBuffer[IntervalEvent]): Unit = {
    var firstStep: IntervalEvent = null
    for (event <- sorted) {
      // start -> end  repeat -> end  start -> repeat
      event.eventType match {
        case EventType.START =>
          if (firstStep == null) firstStep = event
        case EventType.REPEAT =>
          if (firstStep == null) {
            firstStep = event
          } else {
            firstStep.interval = event.ts - firstStep.ts
            if (viewType == VIEW_END) firstStep.groupingInfo = event.groupingInfo
            result.append(firstStep)
            firstStep = null
          }
        case EventType.END if firstStep != null =>
          firstStep.interval = event.ts - firstStep.ts
          if (viewType == VIEW_END) firstStep.groupingInfo = event.groupingInfo
          result.append(firstStep)
          firstStep = null
        case _ =>
      }
    }
  }

  def doNotRepeatVirtualRelEval(sorted: ListBuffer[IntervalEvent],
                             result: ListBuffer[IntervalEvent]): Unit = {
    val map: mutable.HashMap[String, IntervalEvent] = mutable.HashMap[String, IntervalEvent]()
    for (event <- sorted) {
      // start -> end  repeat -> end  start -> repeat
      val startRel = event.relatedDims.apply(0)
      val endRel = event.relatedDims.apply(1)
      event.eventType match {
        case EventType.START =>
          if (startRel != null && !map.contains(startRel)) {
            map(startRel) = event
          }
        case EventType.REPEAT =>
          var isStart = true
          val endRel = event.relatedDims.apply(1)
          if (endRel != null) {
            val startEvent = map.getOrElse(endRel, null)
            if (startEvent != null) {
              startEvent.interval = event.ts - startEvent.ts
              if (viewType == VIEW_END) startEvent.groupingInfo = event.groupingInfo
              result.append(startEvent)
              map -= endRel
              isStart = false
            }
          }
          if(isStart) {
            if (startRel != null && !map.contains(startRel)) {
              map(startRel) = event
            }
          }
        case EventType.END =>
          if (endRel != null) {
            val startEvent = map.getOrElse(endRel, null)
            if (startEvent != null) {
              startEvent.interval = event.ts - startEvent.ts
              if (viewType == VIEW_END) startEvent.groupingInfo = event.groupingInfo
              result.append(startEvent)
              map -= endRel
            }
          }
        case _ =>
      }
    }
  }

  override def eval(buffer: ListBuffer[IntervalEvent]): GenericArrayData = {
    try {
      val result = ListBuffer[IntervalEvent]()
      // aggDate
      buffer.groupBy(_.aggDateType).foreach(group => {
        val sorted = group._2.sortBy(_.ts)
        modelType match {
          case SIMPLE => doSimpleEval(sorted, result)
          case SIMPLE_REL => doSimpleRelEval(sorted, result)
          case REPEAT => doRepeatEval(sorted, result)
          case REPEAT_REL => doRepeatRelEval(sorted, result)
          case NOT_REPEAT_VIRTUAL => doNotRepeatVirtualEval(sorted, result)
          case NOT_REPEAT_VIRTUAL_ERL => doNotRepeatVirtualRelEval(sorted, result)
          case _ =>
        }
      })
      return new GenericArrayData(result.map(e => {
        InternalRow(
          UTF8String.fromString(e.aggDateType),
          UTF8String.fromString(e.groupingInfo),
          e.interval
        )
      }))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    new GenericArrayData(Seq(InternalRow(null, null, null)))
  }

  override def dataType: DataType = {
    if (dataTypeValue != null) {
      return dataTypeValue
    }
    dataTypeValue = ArrayType(
      StructType(Seq(
        StructField("agg_date", StringType),
        StructField("group_info", StringType),
        StructField("interval_ms", LongType)
      )))
    dataTypeValue
  }
  override def serialize(buffer: ListBuffer[IntervalEvent]): Array[Byte] = {
    serializerInstance.serialize(buffer).array()
  }

  override def deserialize(storageFormat: Array[Byte]): ListBuffer[IntervalEvent] = {
    serializerInstance.deserialize(ByteBuffer.wrap(storageFormat))
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = false

  override def children: Seq[Expression] =
    startEventExpr :: endEventExpr :: eventTsExpr :: modelTypeLitExpr ::
      relationsExpr :: viewDimExpr :: viewTypeLitExpr :: aggDateTypeLitExpr :: Nil


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
case class IntervalEvent(ts: Long,
                         eventType: Int = -1,
                         aggDateType: String ,
                         relatedDims: Array[String] ,
                         var groupingInfo: String,
                         var interval: Long = 0
                    ) {
  def typeOrdering: Int = eventType

  // override hashcode otherwise changes in contrib field will affect the hashCode
  override def hashCode(): Int = System.identityHashCode(this)

  override def equals(obj: scala.Any) = obj.hashCode() == this.hashCode()
}

object EventType {
  val NULL: Int = -1
  val START: Int = 0
  val END: Int = 1
  val REPEAT: Int = 2
}