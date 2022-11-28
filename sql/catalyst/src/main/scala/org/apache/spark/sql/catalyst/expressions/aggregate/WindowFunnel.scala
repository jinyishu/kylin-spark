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

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, CreateNamedStruct, Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.expressions.objects.SerializerSupport
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * @param windowLit window size in long
 * @param evtNumExpr number of events
 * @param eventTsCol event ts in long
 * @param evtConds expr to return event id (starting from 0)
 * @param eventRelations expr to return related dim values
 * @param groupingInfoExpr expr to return stepId attach props array
 */
case class WindowFunnel(windowLit: Expression,
                        evtNumExpr: Expression,
                        eventTsCol: Expression,
                        evtConds: Expression,
                        eventRelations: Expression,
                        groupingInfoExpr: Expression,
                        mutableAggBufferOffset: Int = 0,
                        inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ListBuffer[Event]]
    with Serializable with Logging
    with SerializerSupport {

  def this(windowLit: Expression,
           evtNumExpr: Expression,
           eventTsCol: Expression,
           evtConds: Expression,
           eventRelations: Expression,
           groupingInfoExpr: Expression) = {
    this(windowLit, evtNumExpr, eventTsCol, evtConds,
      eventRelations, groupingInfoExpr, 0, 0)
  }

  val kryo: Boolean = true
  lazy val window: Long = windowLit.eval().toString.toLong
  lazy val evtNum: Int = evtNumExpr.eval().toString.toInt
  var dataTypeValue: DataType = null
  var groupDimNames: Seq[String] = null
  override def createAggregationBuffer(): ListBuffer[Event] = ListBuffer[Event]()

  def toIntegerArray(expr: Expression, input: InternalRow): Array[Int] = {
    expr.dataType match {
      case _: NullType =>
        Array(-1)
      case _ =>
        // timezone doesn't really matter here
        val arr = expr.eval(input).toString.split(",").map(_.trim.toInt)
        arr
    }
  }

  def toLong(expr: Expression, input: InternalRow): Long = {
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

  def toString(expr: Expression, input: InternalRow): String = {
    val raw = expr.eval(input)
    if (raw != null) {
      raw.toString
    } else {
      null
    }
  }

  def toGroups(expr: Expression, input: InternalRow): GenericInternalRow = {
    val raw = expr.eval(input).asInstanceOf[GenericInternalRow]
    if(raw != null ) {
      raw
    } else {
      null
    }
  }

  def toGroupNames(): Unit = {
    groupDimNames = groupingInfoExpr.children.filter(e => e.isInstanceOf[CreateNamedStruct])
      .map(e => {
        val split = e.toString.split(",")
        split.apply(1).trim + split.apply(2).trim
      })
  }

  override def update(buffer: ListBuffer[Event], input: InternalRow): ListBuffer[Event] = {
    val eids = toIntegerArray(evtConds, input)
    val ts = toLong(eventTsCol, input)
    if (ts < 0 || eids.apply(0) < 0 ) {
      return buffer
    }
    val stepDimValues: GenericInternalRow = toGroups(groupingInfoExpr, input)
    if ( groupDimNames == null) toGroupNames()

    val groupDim = mutable.HashMap [String, String]()
    for (i <- 0 until groupDimNames.length) {
      val dimName = groupDimNames.apply(i)
      val dimValues = stepDimValues.get(i, ArrayType(StringType)).asInstanceOf[GenericInternalRow]
      val step = dimValues.get(0, StringType).toString.toInt
      val dimValue = dimValues.get(1, StringType)
      val dimValueStr = if (dimValue == null) "null" else dimValue.toString
      if(eids.contains(step)) {
        groupDim.put(dimName, dimValueStr)
      }
    }
    val event = Event(ts, eids, groupDim)
    buffer.append(event)
    buffer
  }

  override def merge(buffer: ListBuffer[Event],
                     input: ListBuffer[Event]): ListBuffer[Event] = {
    buffer ++= input
  }


  override def eval(buffer: ListBuffer[Event]): Any = {
    if (groupDimNames == null) toGroupNames()
    val returnRow = new GenericInternalRow(1 + groupDimNames.size )
    if (buffer.length == 0) {
      returnRow(0) = -1
      return returnRow
    }
    val maxStepEvent = doEval(buffer)

    if (maxStepEvent == null) {
      returnRow(0) = -1
      return returnRow
    }

    returnRow(0) = maxStepEvent.maxStep

    var i = 1
    for (name <- groupDimNames) {
      returnRow(i) = UTF8String.fromString(maxStepEvent.resultGroupDim.get(name).get)
      i+=1
    }
    returnRow
  }
  def doEval(buffer: ListBuffer[Event]): Event = {
    val sorted = buffer.sortBy(_.ts)
    var currentMaxStepEvent: Event = null
    val startEvents: ListBuffer[Event] = ListBuffer[Event]()
    breakable {
      for (event <- sorted) {
        if (event.eids.contains(0)) {
          event.resultGroupDim = mutable.HashMap[String, String]()
          for ((x, y) <- event.groupDim) {
            if (x.startsWith("0")) {
              event.resultGroupDim.put(x, y)
            }
          }
          if (currentMaxStepEvent == null) {
            currentMaxStepEvent = event
          }
          startEvents.append(event)
        }
        breakable {
          for (i <- (0 until startEvents.size).reverse) {
            val startEvent = startEvents.apply(i)
            if (event != startEvent) {
              // 超出窗口期 或者 超出当前最大步骤时间
              if ((event.ts - startEvent.ts) > window  || startEvent.ts < currentMaxStepEvent.ts ) {
                break()
              }
              val nextMaxStep = startEvent.maxStep + 1
              var goAhead: Boolean = false
              for(eid <- event.eids) {
                // 大于下一步骤 可以继续向上计算
                if (eid > nextMaxStep ) goAhead = true
                if (eid == nextMaxStep) { // 等于下一步
                  // 添加下一步的分组
                  val nextStepString = nextMaxStep.toString
                  for ((x, y) <- event.groupDim) {
                    if (x.startsWith(nextStepString)) {
                      startEvent.resultGroupDim.put(x, y)
                    }
                  }
                  startEvent.maxStep = nextMaxStep
                  if (nextMaxStep > currentMaxStepEvent.maxStep) { // 不能 >= ，要记录首次达到的最大值
                    currentMaxStepEvent = startEvent
                  }
                }
              }
              if (!goAhead) { // 没有大于下一步骤 不需要在向上计算
                break()
              }
            }
          }
        }
        if (currentMaxStepEvent != null && (currentMaxStepEvent.maxStep + 1) == evtNum) {
          break()
        }
      }
    }
    currentMaxStepEvent
  }

  override def serialize(buffer: ListBuffer[Event]): Array[Byte] = {
    serializerInstance.serialize(buffer).array()
  }

  override def deserialize(storageFormat: Array[Byte]): ListBuffer[Event] = {
    serializerInstance.deserialize(ByteBuffer.wrap(storageFormat))
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = false

  private def transformDataType(dataType: DataType): String = {
    dataType match {
      case BooleanType => "boolean"
      case ByteType => "tinyint"
      case ShortType => "smallint"
      case IntegerType => "integer"
      case LongType => "long"
      case FloatType => "float"
      case DoubleType => "double"
      case DecimalType.USER_DEFAULT => "decimal"
      case DateType => "date"
      case TimestampType => "timestamp"
      case BinaryType => "binary"
      case _ => "string"
    }
  }
  override def dataType: DataType = {
    if (dataTypeValue != null) {
      return dataTypeValue
    }
//    println("DataType--------------------"+ dt)
    val max_step_id = "{\"name\":\"max_step\",\"type\":\"integer\"," +
      "\"nullable\":false,\"metadata\":{}}"
//    val start_time = "{\"name\":\"start_time\",\"type\":\"long\"," +
//      "\"nullable\":false,\"metadata\":{}}"
    val  sb = new StringBuilder()
    sb.append("{\"type\":\"struct\",\"fields\":[")
      .append(max_step_id).append(",")
//      .append(start_time).append(",")

    if(groupDimNames == null ) toGroupNames()
    for(name <- groupDimNames) {
      sb.append("{\"name\":\"").append(name)
        .append("\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}")
        .append(",")
    }
    sb.setLength(sb.length-1)
    sb.append("]}")
    dataTypeValue = DataType.fromJson(sb.toString())
    dataTypeValue
  }
  override def children: Seq[Expression] =
    windowLit :: eventTsCol :: evtNumExpr :: evtConds :: eventRelations :: groupingInfoExpr :: Nil
}

// scalastyle:off
case class Event(ts: Long,
                 eids: Array[Int],
                 groupDim: mutable.HashMap [String, String],
                 var maxStep: Int = 0,
                 var resultGroupDim: mutable.HashMap [String, String] =null
                 ) {
  def typeOrdering: Long = ts

  override def hashCode(): Int = System.identityHashCode(this)
}

