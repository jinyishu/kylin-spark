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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, CreateMap, Expression}
import org.apache.spark.sql.catalyst.expressions.objects.SerializerSupport
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


/**
 * calculate event attribution
 * @param windowLitExpr window size in millis
 * @param eventTsExpr event ts
 * @param sourceNameExpr source event names, should be in the form of
 *   case when cond1 then name1 when cond2 then name2 ... else null end
 *   note that the name should be as short as possible
 * @param targetNameExpr target event names
 * @param lookAheadNameExpr look-ahead event names
 * @param eventRelationTypeLitExpr event relation type, possible values are
 *   NONE, no related events or look-ahead events
 *   AHEAD_ONLY, look-ahead events with no related dims
 *   TARGET_TO_SOURCE, no look-ahead events and target and
 *                     source events are related
 *   TARGET_TO_AHEAD, look-ahead events and targets
 *                    events are related
 *   TARGET_TO_AHEAD_TO_SOURCE, look-ahead events and targets
 *                    events are related; and look-ahead events
 *                    and source events are related as well
 * @param eventRelations the events relation map in the form of
 *   array(map('event1_name', event_1_dim_expr, 'event2_name', event_2_dim_expr))
 * @param measuresExpr the array of the measure epxr on the target event, in the form of
 *   array(measure_expr)
 * @param modelTypeLitExpr attribution model type, possible values are
 *   FIRST, the target event attributes to the first event in the window
 *   LAST, the target event attributes to the last event in the window
 *   LINEAR, the target event attributes to the all events evenly in the window
 *   POSITION, the target event attributes to the all events in the window,
 *             the first and last events are considered contribute 40% to the target event each,
 *             and the reset of the events evenly contribute total 20%
 *   DECAY, the target event attributes to the all events in the window,
 *          while the nearest events in 7 days are considered contribute 100%,
 *          and second nearest events in 7 days contribute 50% each, so on so forth
 *
 * @param groupingInfoExpr other dimensions of the sources events that are of interest
 * @param mutableAggBufferOffset
 * @param inputAggBufferOffset
 */
case class Attribution(windowLitExpr: Expression,
                       eventTsExpr: Expression,
                       sourceNameExpr: Expression,
                       targetNameExpr: Expression,
                       lookAheadNameExpr: Expression,
                       eventRelationTypeLitExpr: Expression,
                       eventRelations: Expression,
                       measuresExpr: Expression,
                       modelTypeLitExpr: Expression,
                       groupingInfoExpr: Expression,
                       mutableAggBufferOffset: Int = 0,
                       inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ListBuffer[AttrEvent]]
    with Serializable with Logging with SerializerSupport {

  import EvalHelper._

  def this(windowLitExpr: Expression,
           eventTsExpr: Expression,
           sourceNameExpr: Expression,
           targetNameExpr: Expression,
           lookAheadNameExpr: Expression,
           eventRelationTypeLitExpr: Expression,
           targetEventRelatedDimsExpr: Expression,
           measuresExpr: Expression,
           modelTypeLitExpr: Expression,
           groupingInfoExpr: Expression) = {
    this(windowLitExpr, eventTsExpr, sourceNameExpr, targetNameExpr, lookAheadNameExpr,
      eventRelationTypeLitExpr, targetEventRelatedDimsExpr, measuresExpr,
      modelTypeLitExpr, groupingInfoExpr, 0, 0)
  }

  val kryo: Boolean = true

  lazy val window: Long = windowLitExpr.eval().toString.toLong
  lazy val measureCount: Int =
    if (measuresExpr == null) {
      0
    } else {
      measuresExpr.children.length
    }

  lazy val aheadCount: Int =
    if (lookAheadNameExpr == null) {
      0
    } else {
      val s = lookAheadNameExpr.toString
      s.split(" ").filter(_.startsWith("a")).length
    }
  // relation model type
  lazy val eventRelationType: String = eventRelationTypeLitExpr.eval().toString
  val NONE = "NONE"
  val AHEAD_ONLY = "AHEAD_ONLY"
  val TARGET_TO_SOURCE = "TARGET_TO_SOURCE"
  val TARGET_TO_AHEAD = "TARGET_TO_AHEAD"
  val TARGET_TO_AHEAD_TO_SOURCE = "TARGET_TO_AHEAD_TO_SOURCE"

  // attribution model type
  lazy val modelType: String = modelTypeLitExpr.eval().toString
  val FIRST = "FIRST"
  val LAST = "LAST"
  val LINEAR = "LINEAR"
  val POSITION = "POSITION"
  val DECAY = "DECAY"

  // map{evt_name: array{dim_expr}}
  var relationExprs: mutable.HashMap[String, mutable.ArrayBuffer[Expression]] = null
  // map{evt1_name: {evt2_name: array[tuple(idx1, idx2)]}
  // idx corresponds to idx in relationExprs's array
  // the map is bi-directional
  var relations: mutable.HashMap[String,
    mutable.HashMap[String, mutable.ArrayBuffer[(Int, Int)]]] = null

  var dataTypeValue: DataType = null

  override def createAggregationBuffer(): ListBuffer[AttrEvent] = ListBuffer[AttrEvent]()

  private def parserRelations(): Unit = {
    relationExprs = mutable.HashMap[String, mutable.ArrayBuffer[Expression]]()
    relations = mutable.HashMap[String, mutable.HashMap[String, mutable.ArrayBuffer[(Int, Int)]]]()
    val idxMap = mutable.HashMap[String, mutable.HashMap[Expression, Int]]()
    for (elem <- eventRelations.children) {
      val relation = elem.asInstanceOf[CreateMap]
      val evt1 = relation.keys(0).eval().toString
      val evt2 = relation.keys(1).eval().toString
      val expr1 = relation.values(0)
      val expr2 = relation.values(1)

      val idxMap1 = idxMap.getOrElseUpdate(evt1, mutable.HashMap[Expression, Int]())
      val idx1 = if (idxMap1.contains(expr1)) {
        idxMap1(expr1)
      } else {
        val arr1 = relationExprs.getOrElseUpdate(evt1, mutable.ArrayBuffer[Expression]())
        arr1 += expr1
        arr1.length - 1
      }

      val idxMap2 = idxMap.getOrElseUpdate(evt2, mutable.HashMap[Expression, Int]())
      val idx2 = if (idxMap2.contains(expr2)) {
        idxMap2(expr2)
      } else {
        val arr2 = relationExprs.getOrElseUpdate(evt2, mutable.ArrayBuffer[Expression]())
        arr2 += expr2
        arr2.length - 1
      }

      relations.getOrElseUpdate(evt1, mutable.HashMap[String, mutable.ArrayBuffer[(Int, Int)]]())
        .getOrElseUpdate(evt2, mutable.ArrayBuffer[(Int, Int)]()) += Tuple2(idx1, idx2)
      relations.getOrElseUpdate(evt2, mutable.HashMap[String, mutable.ArrayBuffer[(Int, Int)]]())
        .getOrElseUpdate(evt1, mutable.ArrayBuffer[(Int, Int)]()) += Tuple2(idx2, idx1)
    }
  }

  override def update(buffer: ListBuffer[AttrEvent], input: InternalRow): ListBuffer[AttrEvent] = {
    try {
      // convert single row to all possible events
      val names = evalEventNames(input)
      if (names.isEmpty) {
        return buffer
      }
      if (relations == null) parserRelations
      val ts = evalToLong(eventTsExpr, input)
      val events = names.map { case (eventType, name) =>
        val relatedDim = if (relationExprs.contains(name)) {
          relationExprs(name).map(expr => {
            val dim = expr.eval(input)
            if (dim == null) null else dim.toString
          }).toArray
        } else {
          null
        }
        val groupingInfo =
          genericArrayData(groupingInfoExpr.eval(input).asInstanceOf[GenericArrayData])

        eventType match {
          case AttrEvent.TARGET if measureCount > 0 =>
            val measures = evalToArray(measuresExpr, input).map { m =>
              if (m == null) 0 else m.toString.toDouble
            }
            AttrEvent(name, eventType, ts, relatedDim, groupingInfo, measures)
          case _ =>
            AttrEvent(name, eventType, ts, relatedDim, groupingInfo)
        }
      }
      buffer ++= events
    } catch {
      case e: Exception => e.printStackTrace()
    }
    buffer
  }

  // groupingInfoExpr.dataType
  private def genericArrayData(genericArrayData: GenericArrayData): String = {
    if (genericArrayData == null) return null
    val oldArray = genericArrayData.array
    val size = oldArray.size
    if (size == 0) {
      return null
    }
    val sb = new StringBuilder()
    for (i <- 0 until size) {
      sb.append(oldArray.apply(i)).append("|")
    }
    sb.setLength(sb.length-1)
    sb.toString()
  }
  private def evalEventNames(input: InternalRow): Seq[(Int, String)] = {
    (modelType match {
      case NONE | TARGET_TO_SOURCE =>
        Seq(
          (AttrEvent.SOURCE, evalToString(sourceNameExpr, input)),
          (AttrEvent.TARGET, evalToString(targetNameExpr, input))
        )
      case _ =>
        Seq(
          (AttrEvent.SOURCE, evalToString(sourceNameExpr, input)),
          (AttrEvent.TARGET, evalToString(targetNameExpr, input)),
          (AttrEvent.AHEAD, evalToString(lookAheadNameExpr, input))
        )
    }).filter(_._2 != null)
  }

  override def merge(buffer: ListBuffer[AttrEvent],
                     input: ListBuffer[AttrEvent]): ListBuffer[AttrEvent] = {
    buffer ++= input
  }

  override def eval(buffer: ListBuffer[AttrEvent]): GenericArrayData = {
    try {
      return toResultForm(doEval(buffer))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    new GenericArrayData(Seq(InternalRow(null, null, null, null, null)))
  }

  private def doEval(events: ListBuffer[AttrEvent]): ListBuffer[AttrEvent] = {
    if (relations == null) parserRelations
    // reverse sort
    val sorted = events.sortBy(e => (- e.ts, - e.typeOrdering, e.name))

    // queue{target-> queue{source}}
    val targetEvents = mutable.Queue[AttrEvent]()
    val resultEvents = mutable.HashSet[AttrEvent]()
    // count the number of all source events  by group
    val counts: mutable.HashMap[(String, String), (SourceCount, SourceCount)] =
      mutable.HashMap[(String, String), (SourceCount, SourceCount)]()
    var aheadTrueName: mutable.HashSet[String] = null
    var aheadTrue: mutable.ListBuffer[AttrEvent] = null
    for (event <- sorted) {
      // check out of window sequence and do calculation
      while (targetEvents.nonEmpty &&
        (!withinWindow(event, targetEvents.front)
          // valid count
          // || targetEvents.front.last != null
          )) {
        val target = targetEvents.dequeue()
        resultEvents ++= calculateContrib(target)
      }
      event.eventType match {
        case AttrEvent.TARGET =>
          event.sourceEvents = mutable.Stack[AttrEvent]()
          eventRelationType match {
            case AHEAD_ONLY | TARGET_TO_AHEAD | TARGET_TO_AHEAD_TO_SOURCE =>
              event.aheadEvents = mutable.ListBuffer[AttrEvent]()
            case _ =>
          }
          targetEvents.enqueue(event)
        case AttrEvent.SOURCE =>
          // count
          val sc: (SourceCount, SourceCount) = counts.getOrElseUpdate(
            (event.name, event.groupingInfos), (SourceCount(0L), SourceCount(0L))
          )
          sc._1.count += 1
          if (targetEvents.nonEmpty) {
            // valid count
            sc._2.count += 1
          }
          // check and enqueue source events
          for (target <- targetEvents) {
            eventRelationType match {
              case NONE =>
                updateValidSource(event, target)
              case TARGET_TO_SOURCE  if related(target, event) =>
                updateValidSource(event, target)
              case AHEAD_ONLY | TARGET_TO_AHEAD | TARGET_TO_AHEAD_TO_SOURCE =>
                if (aheadTrueName == null) {
                  aheadTrueName = mutable.HashSet[String]()
                  aheadTrue = mutable.ListBuffer[AttrEvent]()
                }
                for (ahead <- target.aheadEvents) {
                  if (related(ahead, event)) {
                    aheadTrueName.add(ahead.name)
                    aheadTrue.append(ahead)
                  }
                }
                if (aheadTrueName.size == aheadCount ) {
                  updateValidSource(event, target)
                  target.aheadEvents = target.aheadEvents.diff( aheadTrue)
                }
                aheadTrueName.clear()
                aheadTrue.clear()
              case _ =>
            }
          }
        case AttrEvent.AHEAD =>
            for (target <- targetEvents) {
              if (eventRelationType == NONE || related(target, event)) {
                target.aheadEvents.append(event)
              }
            }
        case _ =>
      }
    }

    // dequeue and calculate the remaining
    for (target <- targetEvents) {
      resultEvents ++= calculateContrib(target)
    }

    val resultEventsBuffer = ListBuffer[AttrEvent]()
    resultEventsBuffer ++= resultEvents ++= counts.map( e => {
      AttrEvent(e._1._1, -1, e._2._1.count, null, e._1._2, null, e._2._2.count.toDouble)
    })
  }

  private def updateValidSource(event: AttrEvent, target: AttrEvent): Unit = {
    modelType match {
      case FIRST =>
        target.first = event // only update first
        // when sourceEvents is empty will be judged as direct conversion
        // only push once
        if (target.sourceEvents.isEmpty) target.sourceEvents.push(event)
      case LAST =>
        if (target.last == null) target.last = event // only update last
        if (target.sourceEvents.isEmpty) target.sourceEvents.push(event)
      case _ => target.sourceEvents.push(event)
    }
  }

  private def related(event1: AttrEvent, event2: AttrEvent): Boolean = {
    val relation = relations.get(event1.name)
    if (relation == null || relation.isEmpty) {
      return true
    }
    val pairs = relation.get.get(event2.name)
    if (pairs.isEmpty) {
      return true
    }
    for ((idx1, idx2) <- pairs.get) {
      if (event1.relatedDims(idx1) == null || event2.relatedDims(idx2) == null) {
        return false
      }
      if (!event1.relatedDims(idx1).equals(event2.relatedDims(idx2))) {
        return false
      }
    }
    true
  }

  private def calculateContrib(target: AttrEvent): mutable.Stack[AttrEvent] = {
    if (target.sourceEvents.isEmpty) {
      val st = mutable.Stack[AttrEvent]()
      st.push(AttrEvent("d", AttrEvent.SOURCE, 0, Array(), null))
      return st
    }
    modelType match {
      case FIRST =>
        target.first.contrib += 1
        updateMeasures(target, target.first)
        // set right first before return sourceEvents
        target.sourceEvents.clear()
        target.sourceEvents.push(target.first)
      case LAST =>
        target.last.contrib += 1
        updateMeasures(target, target.last)
      case LINEAR =>
        val contrib = 1.0 / target.sourceEvents.size
        target.sourceEvents.foreach { e =>
          e.contrib += contrib
          updateMeasures(target, e)
        }
      case POSITION =>
        if (target.sourceEvents.size == 1) {
          target.sourceEvents.foreach { e =>
            e.contrib += 1
            updateMeasures(target, e)
          }
        } else if (target.sourceEvents.size == 2) {
          target.sourceEvents.foreach { e =>
            e.contrib += 0.5
            updateMeasures(target, e)
          }
        } else {
          val remContrib = 0.2 / (target.sourceEvents.size - 2)
          val first = target.sourceEvents.top
          val last = target.sourceEvents.last
          target.sourceEvents.foreach { e => {
              if (e == first || e == last) {
                e.contrib += 0.4
              } else {
                e.contrib += remContrib
              }
              updateMeasures(target, e)
            }
          }
        }
      case DECAY =>
        var contrib = 1.0
        val oneWeek = 7 * 24 * 60 * 60 * 1000
        var windowEnd = target.ts - oneWeek // first DECAY end time
        target.sourceEvents.reverseIterator.foreach { e =>
          while (e.ts < windowEnd) { //  stride across Multiple DECAY
            windowEnd = windowEnd - oneWeek // next DECAY end time
            contrib *= 0.5
          }
          e.contrib += contrib
          updateMeasures(target, e)
        }
    }
    target.sourceEvents
  }

  private def updateMeasures(target: AttrEvent, source: AttrEvent): Unit = {
    if (target.measureContrib == null || target.measureContrib.length == 0) {
      return
    }
    if (source.measureContrib == null) source.measureContrib = Array.fill(measureCount)(0)
    target.measureContrib.zipWithIndex.foreach {
      case (measure, idx) =>
        source.measureContrib(idx) += measure * source.contrib
    }
  }

  private def withinWindow(event1: AttrEvent, event2: AttrEvent): Boolean = {
    event2.ts - event1.ts <= window
  }


  private def toResultForm(events: ListBuffer[AttrEvent]): GenericArrayData = {
    new GenericArrayData(
      events.map(e =>
          if (e.eventType == -1) { // 返回所有按分组统计待归因事件的数量
            InternalRow(e.utf8Name, -1.0, null,
              if (e.groupingInfos == null) null
              else new GenericArrayData(
                e.groupingInfos.split("\\|").map(s => UTF8String.fromString(s))
              ),
              e.ts, e.contrib.toLong)
          } else {
            InternalRow(e.utf8Name, e.contrib,
              if (e.measureContrib == null) null else new GenericArrayData(e.measureContrib),
              if (e.groupingInfos == null) null
              else new GenericArrayData(
                e.groupingInfos.split("\\|").map(s => UTF8String.fromString(s))
              ),
              null, null
            )
          }
      )
    )
  }

  override def dataType: DataType = {
    if (dataTypeValue != null) {
      return dataTypeValue
    }
    dataTypeValue = ArrayType(
      StructType(Seq(
        StructField("name", StringType),
        StructField("contrib", DoubleType),
        StructField("measureContrib", ArrayType(DoubleType)),
        StructField("groupingInfos", ArrayType(StringType)),
        StructField("allCount", LongType),
        StructField("validCount", LongType)
      )))
    dataTypeValue
  }
  override def serialize(buffer: ListBuffer[AttrEvent]): Array[Byte] = {
    serializerInstance.serialize(buffer).array()
  }

  override def deserialize(storageFormat: Array[Byte]): ListBuffer[AttrEvent] = {
    serializerInstance.deserialize(ByteBuffer.wrap(storageFormat))
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = false

  override def children: Seq[Expression] =
    windowLitExpr ::  eventTsExpr ::  sourceNameExpr ::
      targetNameExpr :: lookAheadNameExpr ::
      eventRelationTypeLitExpr ::
      eventRelations :: measuresExpr ::
      modelTypeLitExpr ::  groupingInfoExpr :: Nil
}

// scalastyle:off
case class SourceCount(var count: Long)
case class AttrEvent(name: String,
                     eventType: Int,
                     ts: Long,
                     relatedDims: Array[String],
                     groupingInfos: String,
                     var measureContrib: Array[Double]= null,
                     var contrib: Double= 0,
                     var first: AttrEvent= null,
                     var last: AttrEvent= null,
                     var sourceEvents: mutable.Stack[AttrEvent]= null,
                     var aheadEvents: mutable.ListBuffer[AttrEvent]= null
                    ) {
  def typeOrdering: Int = eventType

  lazy val utf8Name: UTF8String = UTF8String.fromString(name)

  // override hashcode otherwise changes in contrib field will affect the hashCode
  override def hashCode(): Int = System.identityHashCode(this)

  override def equals(obj: scala.Any) = obj.hashCode() == this.hashCode()
}

object AttrEvent {
  val SOURCE: Int = 0
  val AHEAD: Int = 1
  val TARGET: Int = 2
}


private object EvalHelper {

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

  def evalToArray(expr: Expression, input: InternalRow): Array[Any] = {
    Option(expr.eval(input)) match {
      case Some(arr: GenericArrayData) =>
        arr.asInstanceOf[GenericArrayData].array
      case _ =>
        Array.empty
    }
  }
}
