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

import org.roaringbitmap.longlong.Roaring64NavigableMap

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{CompressBitmapUtils, Expression}
import org.apache.spark.sql.catalyst.expressions.objects.SerializerSupport
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


case class CompressBitmapBuild(child: Expression,
                       mutableAggBufferOffset: Int = 0,
                       inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[Roaring64NavigableMap]
    with Serializable with Logging with SerializerSupport {

  def this(child: Expression) = this(child, 0, 0)

  val kryo: Boolean = true

  override def createAggregationBuffer(): Roaring64NavigableMap = new Roaring64NavigableMap()

  override def update(buffer: Roaring64NavigableMap, input: InternalRow): Roaring64NavigableMap = {
    val colValue = child.eval(input)
    if (colValue != null) {
      colValue match {
        case value: Integer =>
          buffer.addLong(value.asInstanceOf[Integer].longValue())
        case value: Long =>
          buffer.addLong(value.asInstanceOf[Long])
        case _ =>
      }
    }
    buffer
  }


  override def merge(buffer: Roaring64NavigableMap,
                     input: Roaring64NavigableMap): Roaring64NavigableMap = {
    buffer.or(input)
    buffer
  }

  override def eval(buffer: Roaring64NavigableMap): UTF8String = {
    val result = CompressBitmapUtils.bitmapToCompressEncoderString(buffer)
    UTF8String.fromString(result)
  }


  override def dataType: DataType = StringType

  override def serialize(buffer: Roaring64NavigableMap): Array[Byte] = {
    serializerInstance.serialize(buffer).array()
  }

  override def deserialize(storageFormat: Array[Byte]): Roaring64NavigableMap = {
    serializerInstance.deserialize(ByteBuffer.wrap(storageFormat))
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = false

  override def children: Seq[Expression] = child :: Nil
}
