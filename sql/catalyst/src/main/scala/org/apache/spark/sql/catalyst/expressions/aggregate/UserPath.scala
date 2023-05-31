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
import scala.collection.mutable.ListBuffer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.catalyst.expressions.objects.SerializerSupport
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * @param eventTsExpr event time
 * @param eventsExpr user path event
 * @param eventsByExpr user path event group
 * @param eventTypeLit start or end
 * @param eventIdLit start or end event id
 * @param eventFilterExpr start or end event filter
 * @param sessionFieldExpr session field
 * @param sessionIntervalLit session interval
 */
case class UserPath(eventTsExpr: Expression,
                        eventsExpr: Expression,
                        eventsByExpr: Expression,
                        eventTypeLit: Expression,
                        eventIdLit: Expression,
                        eventFilterExpr: Expression,
                        sessionFieldExpr: Expression,
                        sessionIntervalLit: Expression,
                        mutableAggBufferOffset: Int = 0,
                        inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ListBuffer[UserPathEvent]]
    with Serializable with Logging
    with SerializerSupport {

  def this(eventTsExpr: Expression,
           eventsExpr: Expression,
           eventsByExpr: Expression,
           eventTypeLit: Expression,
           eventIdLit: Expression,
           eventFilterExpr: Expression,
           sessionFieldExpr: Expression,
           sessionIntervalLit: Expression) = {
    this(eventTsExpr, eventsExpr, eventsByExpr, eventTypeLit, eventIdLit,
      eventFilterExpr, sessionFieldExpr, sessionIntervalLit, 0, 0)
  }

  val kryo: Boolean = true
  val maxNum: Int = 20
  lazy val modelType: String = eventTypeLit.eval().toString
  lazy val eventId: Int = eventIdLit.eval().toString.toInt
  lazy val sessionInterval: Int = sessionIntervalLit.eval().toString.toInt

  var dataTypeValue: DataType = null
  override def createAggregationBuffer(): ListBuffer[UserPathEvent] = ListBuffer[UserPathEvent]()

  def toInteger(expr: Expression, input: InternalRow): Int = {
    expr.dataType match {
      case _: NullType =>
        -1
      case _ =>
        expr.eval(input).toString.toInt
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

  def toBoolean(expr: Expression, input: InternalRow): Boolean = {
    val raw = expr.eval(input)
    if (raw != null) {
      raw.asInstanceOf[Boolean]
    } else {
      true
    }
  }


  override def update(buffer: ListBuffer[UserPathEvent],
                      input: InternalRow): ListBuffer[UserPathEvent] = {
    try {
      val ts = toLong(eventTsExpr, input)
      if (ts < 0) return buffer
      val eid = toInteger(eventsExpr, input)
      if (eid < 0) return buffer
      val sessionField = toString(sessionFieldExpr, input)
      if (sessionInterval <= 0 && (sessionField == null || sessionField.isEmpty)) return buffer
      val eventsBy = toString(eventsByExpr, input)
      val eventFilter = toBoolean(eventFilterExpr, input)
      val event = UserPathEvent(ts, eid, eventsBy, eventFilter, sessionField)
      buffer.append(event)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    buffer
  }

  override def merge(buffer: ListBuffer[UserPathEvent],
                     input: ListBuffer[UserPathEvent]): ListBuffer[UserPathEvent] = {
    buffer ++= input
  }


  def doEvalSessionField(buffer: ListBuffer[UserPathEvent],
                         result: ListBuffer[UserPathResult]): Unit = {
    buffer.groupBy(_.sessionField).foreach(group => {
      val sid = group._1
      val events = group._2
      var sorted: ListBuffer[UserPathEvent] = null
      if (modelType.equals("START")) {
        // 起始事件 增序排列
        sorted = events.sortBy(e => (e.ts, e.eid))
      } else {
        sorted = events.sortBy(e => (-e.ts, -e.eid))
      }
      var upEvent: UserPathEvent = null
      var seq: Int = 0
      for (event <- sorted) {
        if (event.eid == eventId && event.eidFilter && upEvent == null) {
          upEvent = event // 找到符合要求的起始或结束事件
        } else if (upEvent != null) { // 必须要有上一个事件
          seq += 1
          if(seq < maxNum) {
            // 最多20层
            val re = UserPathResult(
              sid, seq, upEvent.eid, upEvent.eidBy, event.eid, event.eidBy
            )
            result.append(re)
          }
          upEvent = event
        }
      }
      // session 结束 统计流失
      if (upEvent != null) {
        val re = UserPathResult(
          sid, seq + 1, upEvent.eid, upEvent.eidBy, -1, null
        )
        result.append(re)
      }
    })
  }

  def doEvalSessionInterval(buffer: ListBuffer[UserPathEvent],
                            result: ListBuffer[UserPathResult]): Unit = {
    /**
     * 先排序，找到第一个符合要求的起始或结束事件，找到后依次遍历，按时间差切分session
     * 当出现新session时 统计上一个session的流失事件
     */
    var sorted: ListBuffer[UserPathEvent] = null
    if (modelType.equals("START")) {
      // 起始事件 增序排列
      sorted = buffer.sortBy(e => ( e.ts, e.eid))
    } else {
      sorted = buffer.sortBy(e => ( -e.ts, -e.eid))
    }
    var upEvent: UserPathEvent = null
    var seq: Int = 0
    var sid: Int = 0
    for (event <- sorted) {

      if (event.eid == eventId && event.eidFilter && upEvent == null) {
        upEvent = event // 找到符合要求的起始或结束事件
        sid += 1 // session id 自增
      } else if (upEvent != null) { // 必须要有上一个事件
        if ( (upEvent.ts - event.ts).abs < sessionInterval ) {
          // 同一个session
          seq += 1
          if (seq < maxNum) {
            // 最多20层
            val re = UserPathResult(
              sid.toString, seq, upEvent.eid, upEvent.eidBy, event.eid, event.eidBy
            )
            result.append(re)
          }
          // 需要更新该session的最后一个事件
          upEvent = event
        } else {
          // 新的session
          // 统计上一个session的流失事件
          val re = UserPathResult(
            sid.toString, seq + 1, upEvent.eid, upEvent.eidBy, -1, null
          )
          result.append(re)
          // 重置session
          upEvent = null
          seq = 0
          // 再次判断是否符合要求的起始或结束事件
          if (event.eid == eventId && event.eidFilter && upEvent == null) {
            upEvent = event // 找到符合要求的起始或结束事件
            sid += 1 // session id 自增
          }
        }
      }
    }
    // session 结束 统计流失
    if (upEvent != null) {
      val re = UserPathResult(
        sid.toString, seq + 1, upEvent.eid, upEvent.eidBy, -1, null
      )
      result.append(re)
    }

  }

  override def eval(buffer: ListBuffer[UserPathEvent]): GenericArrayData = {
    val result = ListBuffer[UserPathResult]()
    try {
      if (sessionInterval > 0) {
        // 判断是否有session 间隔
        doEvalSessionInterval(buffer, result)
      } else {
        doEvalSessionField(buffer, result)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    toResultForm(result)
  }

  def toResultForm(result: ListBuffer[UserPathResult]): GenericArrayData = {
    new GenericArrayData(
      result.map(e =>
        InternalRow(
          UTF8String.fromString(e.sid),
          e.seqNum,
          e.eid,
          if (e.eidBy != null ) UTF8String.fromString(e.eidBy) else null,
          e.nextEid,
          if (e.nextEidBy != null ) UTF8String.fromString(e.nextEidBy) else null
        )
      )
    )
  }

  override def serialize(buffer: ListBuffer[UserPathEvent]): Array[Byte] = {
    serializerInstance.serialize(buffer).array()
  }

  override def deserialize(storageFormat: Array[Byte]): ListBuffer[UserPathEvent] = {
    serializerInstance.deserialize(ByteBuffer.wrap(storageFormat))
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = false

  override def dataType: DataType = {
    if (dataTypeValue != null) {
      return dataTypeValue
    }
    dataTypeValue = ArrayType(
      StructType(Seq(
        StructField("sid", StringType),
        StructField("seqNum", IntegerType),
        StructField("eid", IntegerType),
        StructField("eidBy", StringType),
        StructField("nextEid", IntegerType),
        StructField("nextEidBy", StringType)
      )))
    dataTypeValue
  }
  override def children: Seq[Expression] =
    eventTsExpr :: eventsExpr :: eventsByExpr :: eventTypeLit ::
      eventIdLit :: eventFilterExpr :: sessionFieldExpr :: sessionIntervalLit :: Nil
}

// scalastyle:off
case class UserPathEvent(ts: Long,
                         eid: Int,
                         eidBy: String,
                         eidFilter: Boolean,
                         sessionField: String
                        ) {
  def typeOrdering: Long = ts
  override def hashCode(): Int = System.identityHashCode(this)
}
case class UserPathResult(sid: String,
                          seqNum: Int,
                          eid: Int,
                          eidBy: String,
                          nextEid:Int,
                          nextEidBy: String
                         )

