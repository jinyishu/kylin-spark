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
 * @param evtNumLit number of events
 * @param modelTypeLit WindowFunnel type ,SIMPLE,SIMPLE_REL,REPEAT,REPEAT_REL
 * @param eventTsExpr event ts in long
 * @param baseGroupExpr start event base group ,day group
 * @param evtConds expr to return event id (starting from 0)
 * @param eventRelations expr to return related dim values
 * @param groupExpr expr to return stepId attach props array
 */
case class WindowFunnel(windowLit: Expression,
                        evtNumLit: Expression,
                        modelTypeLit: Expression,
                        eventTsExpr: Expression,
                        baseGroupExpr: Expression,
                        evtConds: Expression,
                        eventRelations: Expression,
                        groupExpr: Expression,
                        mutableAggBufferOffset: Int = 0,
                        inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ListBuffer[FunnelEvent]]
    with Serializable with Logging
    with SerializerSupport {

  def this(windowLit: Expression,
           evtNumLit: Expression,
           modelTypeLit: Expression,
           eventTsExpr: Expression,
           baseGroupExpr: Expression,
           evtConds: Expression,
           eventRelations: Expression,
           groupExpr: Expression) = {
    this(windowLit, evtNumLit, modelTypeLit, eventTsExpr, baseGroupExpr, evtConds,
      eventRelations, groupExpr, 0, 0)
  }

  val kryo: Boolean = true
  lazy val window: Long = windowLit.eval().toString.toLong
  lazy val evtNum: Int = evtNumLit.eval().toString.toInt
  lazy val baseGroupName: String = "0" + baseGroupExpr.toString.split("#")(0)

  val SIMPLE = "SIMPLE"
  val REPEAT = "REPEAT"
  val SIMPLE_REL = "SIMPLE_REL"
  val REPEAT_REL = "REPEAT_REL"
  lazy val modelType: String = modelTypeLit.eval().toString
  lazy val isRepeat: Boolean = modelType.contains(REPEAT)
  lazy val isRelations: Boolean = modelType.contains("REL")


  var dataTypeValue: DataType = null
  var groupDimNames: Seq[String] = null
  override def createAggregationBuffer(): ListBuffer[FunnelEvent] = ListBuffer[FunnelEvent]()

  def toIntegerArray(expr: Expression, input: InternalRow): Array[Int] = {
    expr.dataType match {
      case _: NullType =>
        Array(-1)
      case _ =>
        val arr = expr.eval(input).toString.split(",").map(_.trim.toInt)
        arr
    }
  }

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

  def toGroups(expr: Expression, input: InternalRow): GenericInternalRow = {
    val raw = expr.eval(input).asInstanceOf[GenericInternalRow]
    if(raw != null ) {
      raw
    } else {
      null
    }
  }

  def toGroupNames: Seq[String] = {
    groupExpr.children.filter(e => e.isInstanceOf[CreateNamedStruct])
      .map(e => {
        val split = e.toString.split(",")
        split.apply(1).trim + split.apply(2).trim
      }).filter(!_.equals(baseGroupName))
  }

  override def update(buffer: ListBuffer[FunnelEvent],
                      input: InternalRow): ListBuffer[FunnelEvent] = {
    try {
      val ts = toLong(eventTsExpr, input)
      if (ts < 0) return buffer

      var eids: Array[Int] = null
      var eid: Int = -1
      var baseGroup: String = null
      var upRelations: String = null
      var downRelations: String = null
      var relations: GenericInternalRow = null
      var repeatUpRelations: mutable.HashMap[Int, String] = null
      var repeatDownRelations: mutable.HashMap[Int, String] = null
      if (isRelations) {
        relations = eventRelations.eval(input).asInstanceOf[GenericInternalRow]
      }

      if (isRepeat) {
        eids = toIntegerArray(evtConds, input)
        if (eids.apply(0) < 0) return buffer
        if (eids.contains(0)) baseGroup = toString(baseGroupExpr, input)
        if (isRelations) {
          repeatUpRelations = mutable.HashMap[Int, String]()
          repeatDownRelations = mutable.HashMap[Int, String]()
          for (id <- eids) {
            val up = relations.values(id).asInstanceOf[GenericInternalRow].values(0)
            if (up != null) repeatUpRelations.put(id, up.toString)
            val down = relations.values(id).asInstanceOf[GenericInternalRow].values(1)
            if (down != null) repeatDownRelations.put(id, down.toString)
          }

        }
      } else {
        eid = toInteger(evtConds, input)
        if (eid < 0) return buffer
        if (eid == 0) {
          baseGroup = toString(baseGroupExpr, input)
          if (baseGroup == null) baseGroup = "null"
        }
        if (isRelations) {
          val up = relations.values(eid).asInstanceOf[GenericInternalRow].values(0)
          if (up != null) upRelations = up.toString
          val down = relations.values(eid).asInstanceOf[GenericInternalRow].values(1)
          if (down != null) downRelations = down.toString
        }
      }

      var groupDim: mutable.HashMap[String, String] = null
      if (groupDimNames == null) groupDimNames = toGroupNames
      if (groupDimNames.nonEmpty) {
        groupDim = mutable.HashMap[String, String]()
        val stepDimValues: GenericInternalRow = toGroups(groupExpr, input)
        for (i <- groupDimNames.indices) {
          val dimName = groupDimNames.apply(i)
          val dimValues = stepDimValues.get(i, ArrayType(StringType))
            .asInstanceOf[GenericInternalRow]
          val stepStr = dimValues.get(0, StringType).toString
          val dimValue = dimValues.get(1, StringType)
          val dimValueStr = if (dimValue == null) "null" else dimValue.toString
          if (stepStr.startsWith("user")) { // user or user_group
            groupDim.put(dimName, dimValueStr)
          } else {
            val step = stepStr.toInt
            if (isRepeat) {
              if (eids.contains(step)) groupDim.put(dimName, dimValueStr)
            } else {
              if (eid == step) groupDim.put(dimName, dimValueStr)
            }
          }
        }
      }

      val event = FunnelEvent(ts, eid, eids, baseGroup, groupDim,
        upRelations, downRelations, repeatUpRelations, repeatDownRelations)
      buffer.append(event)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    buffer
  }

  override def merge(buffer: ListBuffer[FunnelEvent],
                     input: ListBuffer[FunnelEvent]): ListBuffer[FunnelEvent] = {
    buffer ++= input
  }


  override def eval(buffer: ListBuffer[FunnelEvent]): Any = {
    if (groupDimNames == null) groupDimNames = toGroupNames
    val resultRow = new GenericInternalRow(2 + groupDimNames.size )
    if (buffer.isEmpty) {
      returnDefaultRow(resultRow)
      return resultRow
    }
    try {
      val sorted = buffer.sortBy(_.ts)
      val maxStepEvent = modelType match {
        case SIMPLE => doSimpleEval(sorted)
        case SIMPLE_REL => doSimpleRelationsEval(sorted)
        case REPEAT => doRepeatEval(sorted)
        case REPEAT_REL => doRepeatRelationsEval(sorted)
        case _ => null
      }
      if (maxStepEvent == null) {
        returnDefaultRow(resultRow)
        return resultRow
      }
      resultRow(0) = maxStepEvent.maxStep
      resultRow(1) = UTF8String.fromString(maxStepEvent.baseGroup)
      var i = 2
      for (name <- groupDimNames) {
        val value = maxStepEvent.resultGroupDim.getOrElse(name, "null")
        resultRow(i) = UTF8String.fromString(value)
        i+=1
      }
      return resultRow
    } catch {
      case e: Exception => e.printStackTrace()
    }
    returnDefaultRow(resultRow)
    resultRow
  }

  private def returnDefaultRow(resultRow: GenericInternalRow): Unit = {
    resultRow(0) = -1
    resultRow(1) = UTF8String.fromString("null")
    var i = 2
    for (name <- groupDimNames) {
      resultRow(i) = UTF8String.fromString("null")
      i += 1
    }
  }
  def calculateFunnel(currentMaxStepEvent: FunnelEvent): FunnelEvent = {
    if (currentMaxStepEvent == null || currentMaxStepEvent.maxStep==0) return currentMaxStepEvent
    // Get the lowest and earliest event in the largest step collection
    var maxStepEvent = currentMaxStepEvent.relationsMapArray(currentMaxStepEvent.maxStep).minBy(_.ts)
    // Set maxStepEvent grouping information
    val maxStepEventId = currentMaxStepEvent.maxStep.toString
    for ((x, y) <- maxStepEvent.groupDim) {
      if (x.startsWith(maxStepEventId) || x.startsWith("user")) {
        currentMaxStepEvent.resultGroupDim.put(x, y)
      }
    }
    // Find the matching path from the largest step up
    for(i <- (1 until currentMaxStepEvent.maxStep).reverse) {
      val stepArray = currentMaxStepEvent.relationsMapArray(i)
      val closerEvent = if (isRepeat) stepArray.filter(e => {
          val downValue = e.repeatDownRelations.get(i)
          val upValue = maxStepEvent.repeatUpRelations.get(i + 1)
          downValue!=null && upValue!=null &&
          e != maxStepEvent && // filter  repeat itself
          e.ts < maxStepEvent.ts && // filter  time
          downValue.equals(upValue) // filter matched relations
        })
        .maxBy(_.ts)
      else stepArray.filter(e => {
          e != maxStepEvent && // filter  repeat itself
          e.ts < maxStepEvent.ts && // filter  time
          e.downRelations.equals(maxStepEvent.upRelations) // filter matched relations
        })
        .maxBy(_.ts) // max time , get closer to the next step

      for ((x, y) <- closerEvent.groupDim) {
        // i.toString： Grouping dimension of current step id
        if (x.startsWith(i.toString) || x.startsWith("user")) {
          currentMaxStepEvent.resultGroupDim.put(x, y)
        }
      }
      maxStepEvent = closerEvent
    }
    currentMaxStepEvent
  }

  def doSimpleRelationsEval(sorted: ListBuffer[FunnelEvent]): FunnelEvent = {
    var currentMaxStepEvent: FunnelEvent = null
    val startEvents = ListBuffer[FunnelEvent]()
    for (event <- sorted) {
      if (event.eid == 0) {
        event.relationsMapArray = mutable.HashMap[Int, ListBuffer[FunnelEvent]]()
        startEvents.append(event)
        if (event.groupDim != null) {
          event.resultGroupDim = mutable.HashMap[String, String]()
          for ((x, y) <- event.groupDim) {
            if (x.startsWith("0") || x.startsWith("user")) {
              event.resultGroupDim.put(x, y)
            }
          }
        }
        if (currentMaxStepEvent == null) {
          currentMaxStepEvent = event
        }
      } else {
        breakable { // only when exceed window , break
          // Each event must match all starting events
          // Subsequent events of each event may associate the attributes of all steps
          for (i <- startEvents.indices.reverse) {
            val startEvent = startEvents.apply(i)
            // The window period is exceeded or the current max step time is exceeded
            if ((event.ts - startEvent.ts) > window) {
              break()
            }
            // get up step id
            val upStepId = event.eid - 1
            if (upStepId == 0 ) {
              if ( startEvent.downRelations != null && event.upRelations != null &&
                startEvent.downRelations.equals(event.upRelations)) {
                startEvent.relationsMapArray.getOrElseUpdate(event.eid, ListBuffer())
                  .append(event)
                // update max step id
                if (event.eid > startEvent.maxStep) startEvent.maxStep = event.eid
                // update max step event
                if (startEvent.maxStep > currentMaxStepEvent.maxStep) {
                  currentMaxStepEvent = startEvent
                }
                if (currentMaxStepEvent.maxStep + 1 == evtNum) {
                  return calculateFunnel(currentMaxStepEvent)
                }
              }
            } else {
              // get all up step id events
              val upStepArray: ListBuffer[FunnelEvent] = startEvent.relationsMapArray.getOrElse(upStepId, null)
              if (upStepArray != null) {
                breakable {
                  for (upStep <- upStepArray) {
                    // The associated attribute is matched successfully.
                    // Save the event to the next collection
                    if (upStep.downRelations != null && event.upRelations != null &&
                      upStep.downRelations.equals(event.upRelations)) {
                      startEvent.relationsMapArray.getOrElseUpdate(event.eid, ListBuffer())
                        .append(event)
                      // update max step id
                      if (event.eid > startEvent.maxStep) startEvent.maxStep = event.eid
                      // update max step event
                      if (startEvent.maxStep > currentMaxStepEvent.maxStep) {
                        currentMaxStepEvent = startEvent
                      }
                      if (currentMaxStepEvent.maxStep + 1 == evtNum) {
                        return calculateFunnel(currentMaxStepEvent)
                      }
                      break()
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    calculateFunnel(currentMaxStepEvent)
  }

  def doSimpleEval(sorted: ListBuffer[FunnelEvent]): FunnelEvent = {
    var currentMaxStepEvent: FunnelEvent = null
    val startEvents: ListBuffer[FunnelEvent] = ListBuffer[FunnelEvent]()
    for (event <- sorted) {
      if (event.eid == 0) {
        if (event.groupDim != null) {
          event.resultGroupDim = mutable.HashMap[String, String]()
          for ((x, y) <- event.groupDim) {
            if (x.startsWith("0") || x.startsWith("user")) {
              event.resultGroupDim.put(x, y)
            }
          }
        }
        if (currentMaxStepEvent == null) {
          currentMaxStepEvent = event
        }
        startEvents.append(event)
      } else {
        breakable {
          for (i <- startEvents.indices.reverse) {
            val startEvent = startEvents.apply(i)
            // The window period is exceeded or the current max step time is exceeded
            if ((event.ts - startEvent.ts) > window || startEvent.ts < currentMaxStepEvent.ts) {
              break()
            }
            val nextMaxStep = startEvent.maxStep + 1
            var upward: Boolean = false
            // greater than the next max step, it can be calculated upward
            if (event.eid > nextMaxStep) upward = true
            if (event.eid == nextMaxStep) {
              if (event.groupDim != null) {
                val nextStepString = nextMaxStep.toString
                for ((x, y) <- event.groupDim) {
                  if (x.startsWith(nextStepString) || x.startsWith("user")) {
                    startEvent.resultGroupDim.put(x, y)
                  }
                }
              }
              startEvent.maxStep = nextMaxStep
              if (nextMaxStep >= currentMaxStepEvent.maxStep) {
                // >= Find the closest to the target event
                currentMaxStepEvent = startEvent
              }
              if (currentMaxStepEvent.maxStep + 1 == evtNum) {
                return currentMaxStepEvent
              }
            }
            if (!upward) {
              // No greater than the next max step, no upward calculation is required
              break()
            }
          }
        }
      }
    }
    currentMaxStepEvent
  }

  def doRepeatEval(sorted: ListBuffer[FunnelEvent]): FunnelEvent = {
    var currentMaxStepEvent: FunnelEvent = null
    val startEvents: ListBuffer[FunnelEvent] = ListBuffer[FunnelEvent]()
    for (event <- sorted) {
      if (event.eids.contains(0)) {
        if (event.groupDim != null) {
          event.resultGroupDim = mutable.HashMap[String, String]()
          for ((x, y) <- event.groupDim) {
            if (x.startsWith("0")  || x.startsWith("user")) {
              event.resultGroupDim.put(x, y)
            }
          }
        }
        if (currentMaxStepEvent == null) {
          currentMaxStepEvent = event
        }
        startEvents.append(event)
      }
      breakable {
        for (i <- startEvents.indices.reverse) {
          val startEvent = startEvents.apply(i)
          if (event != startEvent) {
            if ((event.ts - startEvent.ts) > window || startEvent.ts < currentMaxStepEvent.ts) {
              break()
            }
            val nextMaxStep = startEvent.maxStep + 1
            var upward: Boolean = false
            for (eid <- event.eids) {
              if (eid > nextMaxStep) upward = true
              if (eid == nextMaxStep) {
                if (event.groupDim != null) {
                  val nextStepString = nextMaxStep.toString
                  for ((x, y) <- event.groupDim) {
                    if (x.startsWith(nextStepString)  || x.startsWith("user")) {
                      startEvent.resultGroupDim.put(x, y)
                    }
                  }
                }
                startEvent.maxStep = nextMaxStep
                if (nextMaxStep > currentMaxStepEvent.maxStep) {
                  // do not >= , Repeated events may be missed
                  currentMaxStepEvent = startEvent
                }
                if (currentMaxStepEvent.maxStep + 1 == evtNum) {
                  return currentMaxStepEvent
                }
              }
            }
            if (!upward) {
              break()
            }
          }
        }
      }
    }
    currentMaxStepEvent
  }
  def doRepeatRelationsEval(sorted: ListBuffer[FunnelEvent]): FunnelEvent = {
    var currentMaxStepEvent: FunnelEvent = null
    val startEvents = ListBuffer[FunnelEvent]()
    for (event <- sorted) {
      if (event.eids.contains(0)) {
        event.relationsMapArray = mutable.HashMap[Int, ListBuffer[FunnelEvent]]()
        startEvents.append(event)
        if (event.groupDim != null) {
          event.resultGroupDim = mutable.HashMap[String, String]()
          for ((x, y) <- event.groupDim) {
            if (x.startsWith("0") || x.startsWith("user")) {
              event.resultGroupDim.put(x, y)
            }
          }
        }
        if (currentMaxStepEvent == null) {
          currentMaxStepEvent = event
        }
      }
      breakable {
        for (i <- startEvents.indices.reverse) {
          val startEvent = startEvents.apply(i)
          if(event != startEvent) {
            if ((event.ts - startEvent.ts) > window) {
              break()
            }
            // Traverse each ID ,Match the associated attributes with the previous step
            for(id <- event.eids) {
              // get up step id
              val upStepId = id - 1
              if (upStepId == 0) {
                val startEventDownValue = startEvent.repeatDownRelations.get(0)
                val eventUpValue = event.repeatUpRelations.get(id)
                if (startEventDownValue != null && eventUpValue != null
                  && startEventDownValue.equals(eventUpValue)) {
                  startEvent.relationsMapArray.getOrElseUpdate(id, ListBuffer())
                    .append(event)
                  // update max step id
                  if (id > startEvent.maxStep) startEvent.maxStep = id
                  // update max step event
                  if (startEvent.maxStep > currentMaxStepEvent.maxStep) {
                    currentMaxStepEvent = startEvent
                  }
                  if (currentMaxStepEvent.maxStep + 1 == evtNum) {
                    return calculateFunnel(currentMaxStepEvent)
                  }
                }
              } else {
                // get all up step id events
                val upStepArray: ListBuffer[FunnelEvent] = startEvent.relationsMapArray.getOrElse(upStepId, null)
                if (upStepArray != null) {
                  breakable {
                    for (upStep <- upStepArray) {
                      // The associated attribute is matched successfully.
                      // Save the event to the next collection
                      val upStepDownValue = upStep.repeatDownRelations.get(upStepId)
                      val eventUpValue = event.repeatUpRelations.get(id)
                      if (upStepDownValue != null && eventUpValue != null &&
                        upStep != event && upStepDownValue.equals(eventUpValue)) {
                        startEvent.relationsMapArray.getOrElseUpdate(id, ListBuffer())
                          .append(event)
                        // update max step id
                        if (id > startEvent.maxStep) startEvent.maxStep = id
                        // update max step event
                        if (startEvent.maxStep > currentMaxStepEvent.maxStep) {
                          currentMaxStepEvent = startEvent
                        }
                        if (currentMaxStepEvent.maxStep + 1 == evtNum) {
                          return calculateFunnel(currentMaxStepEvent)
                        }
                        break()
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    calculateFunnel(currentMaxStepEvent)
  }
  override def serialize(buffer: ListBuffer[FunnelEvent]): Array[Byte] = {
    serializerInstance.serialize(buffer).array()
  }

  override def deserialize(storageFormat: Array[Byte]): ListBuffer[FunnelEvent] = {
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
    val max_step_id = "{\"name\":\"max_step\",\"type\":\"integer\"," +
      "\"nullable\":false,\"metadata\":{}}"
    val base_group_name = "{\"name\":\"" + baseGroupName + "\",\"type\":\"string\"," +
      "\"nullable\":false,\"metadata\":{}}"
    val sb: mutable.StringBuilder = new mutable.StringBuilder()
    sb.append("{\"type\":\"struct\",\"fields\":[")
      .append(max_step_id).append(",")
      .append(base_group_name).append(",")

    if(groupDimNames == null ) groupDimNames = toGroupNames
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
    windowLit :: evtNumLit :: modelTypeLit :: eventTsExpr :: baseGroupExpr ::
      evtConds :: eventRelations :: groupExpr :: Nil
}

// scalastyle:off
case class FunnelEvent(ts: Long,
                 var eid: Int = -1,
                 var eids: Array[Int],
                 var baseGroup: String,
                 var groupDim: mutable.HashMap [String, String],
                 var upRelations: String = null,
                 var downRelations: String = null,
                 var repeatUpRelations: mutable.HashMap [Int, String] = null,
                 var repeatDownRelations: mutable.HashMap [Int, String] = null,
                 var maxStep: Int = 0,
                 var resultGroupDim: mutable.HashMap[String, String] = null,
                 var relationsMapArray: mutable.HashMap[Int, ListBuffer[FunnelEvent]] = null
                 ) {
  def typeOrdering: Long = ts

  override def hashCode(): Int = System.identityHashCode(this)
}

