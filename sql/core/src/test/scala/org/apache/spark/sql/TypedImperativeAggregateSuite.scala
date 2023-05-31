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

package org.apache.spark.sql

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.spark.sql.TypedImperativeAggregateSuite.TypedMax
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, GenericInternalRow, ImplicitCastInputTypes, SpecificInternalRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class TypedImperativeAggregateSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private val random = new java.util.Random()

  private val data = (0 until 1000).map { _ =>
    (random.nextInt(10), random.nextInt(100))
  }

  test("aggregate with object aggregate buffer") {
    val agg = new TypedMax(BoundReference(0, IntegerType, nullable = false))

    val group1 = (0 until data.length / 2)
    val group1Buffer = agg.createAggregationBuffer()
    group1.foreach { index =>
      val input = InternalRow(data(index)._1, data(index)._2)
      agg.update(group1Buffer, input)
    }

    val group2 = (data.length / 2 until data.length)
    val group2Buffer = agg.createAggregationBuffer()
    group2.foreach { index =>
      val input = InternalRow(data(index)._1, data(index)._2)
      agg.update(group2Buffer, input)
    }

    val mergeBuffer = agg.createAggregationBuffer()
    agg.merge(mergeBuffer, group1Buffer)
    agg.merge(mergeBuffer, group2Buffer)

    assert(mergeBuffer.value == data.map(_._1).max)
    assert(agg.eval(mergeBuffer) == data.map(_._1).max)

    // Tests low level eval(row: InternalRow) API.
    val row = new GenericInternalRow(Array(mergeBuffer): Array[Any])

    // Evaluates directly on row consist of aggregation buffer object.
    assert(agg.eval(row) == data.map(_._1).max)
  }

  test("supports SpecificMutableRow as mutable row") {
    val aggregationBufferSchema = Seq(IntegerType, LongType, BinaryType, IntegerType)
    val aggBufferOffset = 2
    val buffer = new SpecificInternalRow(aggregationBufferSchema)
    val agg = new TypedMax(BoundReference(ordinal = 1, dataType = IntegerType, nullable = false))
      .withNewMutableAggBufferOffset(aggBufferOffset)

    agg.initialize(buffer)
    data.foreach { kv =>
      val input = InternalRow(kv._1, kv._2)
      agg.update(buffer, input)
    }
    assert(agg.eval(buffer) == data.map(_._2).max)
  }

  test("dataframe aggregate with object aggregate buffer, should not use HashAggregate") {
    val df = data.toDF("a", "b")
    val max = TypedMax($"a".expr)

    // Always uses SortAggregateExec
    val sparkPlan = df.select(Column(max.toAggregateExpression())).queryExecution.sparkPlan
    assert(!sparkPlan.isInstanceOf[HashAggregateExec])
  }

  test("dataframe aggregate with object aggregate buffer, no group by") {
    val df = data.toDF("key", "value").coalesce(2)
    val query = df.select(typedMax($"key"), count($"key"), typedMax($"value"), count($"value"))
    val maxKey = data.map(_._1).max
    val countKey = data.size
    val maxValue = data.map(_._2).max
    val countValue = data.size
    val expected = Seq(Row(maxKey, countKey, maxValue, countValue))
    checkAnswer(query, expected)
  }

  test("dataframe aggregate with object aggregate buffer, non-nullable aggregator") {
    val df = data.toDF("key", "value").coalesce(2)

    // Test non-nullable typedMax
    val query = df.select(typedMax(lit(null)), count($"key"), typedMax(lit(null)),
      count($"value"))

    // typedMax is not nullable
    val maxNull = Int.MinValue
    val countKey = data.size
    val countValue = data.size
    val expected = Seq(Row(maxNull, countKey, maxNull, countValue))
    checkAnswer(query, expected)
  }

  test("dataframe aggregate with object aggregate buffer, nullable aggregator") {
    val df = data.toDF("key", "value").coalesce(2)

    // Test nullable nullableTypedMax
    val query = df.select(nullableTypedMax(lit(null)), count($"key"), nullableTypedMax(lit(null)),
      count($"value"))

    // nullableTypedMax is nullable
    val maxNull = null
    val countKey = data.size
    val countValue = data.size
    val expected = Seq(Row(maxNull, countKey, maxNull, countValue))
    checkAnswer(query, expected)
  }

  test("dataframe aggregation with object aggregate buffer, input row contains null") {

    val nullableData = (0 until 1000).map {id =>
      val nullableKey: Integer = if (random.nextBoolean()) null else random.nextInt(100)
      val nullableValue: Integer = if (random.nextBoolean()) null else random.nextInt(100)
      (nullableKey, nullableValue)
    }

    val df = nullableData.toDF("key", "value").coalesce(2)
    val query = df.select(typedMax($"key"), count($"key"), typedMax($"value"),
      count($"value"))
    val maxKey = nullableData.map(_._1).filter(_ != null).max
    val countKey = nullableData.map(_._1).count(_ != null)
    val maxValue = nullableData.map(_._2).filter(_ != null).max
    val countValue = nullableData.map(_._2).count(_ != null)
    val expected = Seq(Row(maxKey, countKey, maxValue, countValue))
    checkAnswer(query, expected)
  }

  test("dataframe aggregate with object aggregate buffer, with group by") {
    val df = data.toDF("value", "key").coalesce(2)
    val query = df.groupBy($"key").agg(typedMax($"value"), count($"value"), typedMax($"value"))
    val expected = data.groupBy(_._2).toSeq.map { group =>
      val (key, values) = group
      val valueMax = values.map(_._1).max
      val countValue = values.size
      Row(key, valueMax, countValue, valueMax)
    }
    checkAnswer(query, expected)
  }

  test("dataframe aggregate with object aggregate buffer, empty inputs, no group by") {
    val empty = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      empty.select(typedMax($"a"), count($"a"), typedMax($"b"), count($"b")),
      Seq(Row(Int.MinValue, 0, Int.MinValue, 0)))
  }

  test("dataframe aggregate with object aggregate buffer, empty inputs, with group by") {
    val empty = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      empty.groupBy($"b").agg(typedMax($"a"), count($"a"), typedMax($"a")),
      Seq.empty[Row])
  }

  test("TypedImperativeAggregate should not break Window function") {
    val df = data.toDF("key", "value")
    // OVER (PARTITION BY a ORDER BY b ROW BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    val w = Window.orderBy("value").partitionBy("key").rowsBetween(Long.MinValue, 0)

    val query = df.select(sum($"key").over(w), typedMax($"key").over(w), sum($"value").over(w),
      typedMax($"value").over(w))

    val expected = data.groupBy(_._1).toSeq.flatMap { group =>
      val (key, values) = group
      val sortedValues = values.map(_._2).sorted

      var outputRows = Seq.empty[Row]
      var i = 0
      while (i < sortedValues.size) {
        val unboundedPrecedingAndCurrent = sortedValues.slice(0, i + 1)
        val sumKey = key * unboundedPrecedingAndCurrent.size
        val maxKey = key
        val sumValue = unboundedPrecedingAndCurrent.sum
        val maxValue = unboundedPrecedingAndCurrent.max

        outputRows :+= Row(sumKey, maxKey, sumValue, maxValue)
        i += 1
      }

      outputRows
    }
    checkAnswer(query, expected)
  }

  private def typedMax(column: Column): Column = {
    val max = TypedMax(column.expr, nullable = false)
    Column(max.toAggregateExpression())
  }

  private def nullableTypedMax(column: Column): Column = {
    val max = TypedMax(column.expr, nullable = true)
    Column(max.toAggregateExpression())
  }

  test("single attribution test") {
    {
      val colNames = Seq("uid", "eid", "dim1", "dim2", "measure1", "measure2", "ts")

      Seq(
        (1, 1, null, "foo", 0, 0, 1),
        (1, 2, "bar", null, 0, 0, 2),
        (1, 1, "bar", "foo", 0, 0, 3),
        (1, 3, "bar1", null, 0, 0, 4),
        (1, 5, "bar1", "foo", 0, 0, 5), // ahead
        (1, 4, "bar", null, 0, 0, 6),
        (1, 6, "bar1", "foo", 0, 0, 7), // ahead
        (1, 5, "bar", "foo", 0, 0, 8), // ahead
        (1, 6, "bar", "foo", 0, 0, 9), // ahead
        (1, 7, null, "foo", 100, 0, 10),
        (1, 2, "bar1", null, 0, 0, 11),
        (1, 6, "bar1", "foo", 0, 0, 12), // ahead
        (1, 1, "bar1", "foo", 0, 0, 13),
        (1, 5, "bar1", "foo", 0, 0, 14), // ahead
        (1, 7, null, "foo", 100, 0, 15),
        (1, 7, "bar7", "foo", 100, 0, 1000) // Direct conversion
      ).toDF(colNames: _*).createOrReplaceTempView("events")
      val result = "[d,null,null,0,1,0,1,0.0,null];" +
        "[s1,bar,foo,1,0,1,0,0.0,0.0];" +
        "[s1,bar1,foo,1,0,1,0,0.0,0.0];" +
        "[s1,null,foo,1,0,1,0,0.0,0.0];" +
        "[s2,bar1,null,1,1,1,1,1.0,100.0];" +
        "[s2,bar,null,1,0,1,0,0.0,0.0];" +
        "[s3,bar1,null,1,0,1,0,0.0,0.0];" +
        "[s4,bar,null,1,1,1,1,1.0,100.0]"
      val sql = "select \n" +
        "event.name, --  s1 s2 s3 s4 d, d -> Direct conversion \n" +
        "event.groupingInfos[0] by1, -- maybe null\n" +
        "event.groupingInfos[1] by2,-- maybe null\n" +
        "sum(case when event.contrib = -1.0 then event.allCount else 0 end) count_pv_all, \n" +
        "sum(\n" +
        "case when event.contrib = -1.0 then event.validCount else 0 end \n" +
        ") count_pv_valid,  \n" +
        "count(\n" +
        "distinct case when event.contrib = -1.0 then uid else null end\n" +
        ") count_uv_all,  \n" +
        "count(\n" +
        "distinct case " +
        "when event.name = 'd' then null " +
        "when event.contrib > -1.0 then uid else null end\n" +
        ") count_uv_valid, \n" +
        "sum(case when event.contrib > -1.0 then event.contrib else 0 end) sum_contrib,  \n" +
        "sum(\n" +
        "case when event.contrib > -1.0 then event.measureContrib[0] else 0 end \n" +
        ") sum_measure_contrib -- maybe null \n" +
        "from ( \n" +
        "select uid, explode(at) as event from (\n" +
        "select \n" +
        "uid, \n" +
        "attribution_analysis(\n" +
        "100, \n" +
        "ts, \n" +
        "case \n" +
        "when eid=1 then 's1' \n" +
        "when eid=2 then 's2' \n" +
        "when eid=3 then 's3' \n" +
        "when eid=4 then 's4' else null end  , \n" +
        "case when eid=7 then 't7' else null end  ,  \n" +
        "case \n" +
        "when eid=5 then 'a5'\n" +
        "when eid=6 then 'a6' else null end  , -- must start with a \n" +
        "'TARGET_TO_AHEAD_TO_SOURCE',  -- NONE AHEAD_ONLY TARGET_TO_SOURCE TARGET_TO_AHEAD\n" +
        "array(  \n" +
        "map('t7', dim2, 'a5', dim2),\n" +
        "map('t7', dim2, 'a6', dim2),\n" +
        "map('a5', dim1, 's1', dim1),\n" +
        "map('a5', dim1, 's2', dim1),\n" +
        "map('a5', dim1, 's3', dim1),\n" +
        "map('a5', dim1, 's4', dim1),\n" +
        "map('a6', dim1, 's1', dim1),\n" +
        "map('a6', dim1, 's2', dim1),\n" +
        "map('a6', dim1, 's3', dim1),\n" +
        "map('a6', dim1, 's4', dim1)\n" +
        "),\n" +
        "array(measure1),   \n" +
        "'LAST', -- FIRST, LAST, LINEAR, POSITION, DECAY\n" +
        "array(dim1,dim2) \n" +
        ") as at \n" +
        "from events group by uid) \n" +
        ") where event.name is not null " +
        "group by 1,2,3 order by name,count_pv_all"
//      println(sql)
      val df = spark.sql(sql)
       df.show(false)
//      val actual = df.collect().mkString(";")
//       println(actual)
//      assert(result == actual)
    }
  }

  test("test attribution") {
    val oneWeek = 604800000 // 7 * 60 * 60 * 24 * 1000
    val colNames = Seq("uid", "eid", "dim1", "dim2", "measure1", "measure2", "ts")

    //  test attribution models
    {
      Seq(
        (1, 1, null, null, 0, 0, 1),
        (1, 2, null, null, 0, 0, 2),
        (1, 3, null, null, 0, 0, 3),
        (1, 4, null, null, 0, 0, 1 + oneWeek),
        (1, 5, null, null, 100, 1000, 5 + oneWeek)
      ).toDF(colNames: _*).createOrReplaceTempView("events")

      val model = Seq("FIRST", "LAST", "LINEAR", "POSITION", "DECAY")
      val result = Seq(
        // FIRST
        "[[s1,-1.0,null,null,1]];" +
          "[[s1,1.0,WrappedArray(100.0, 1000.0),null,1]];" +
          "[[s2,-1.0,null,null,1]];" +
          "[[s3,-1.0,null,null,1]];" +
          "[[s4,-1.0,null,null,1]]"
        // LAST
        , "[[s1,-1.0,null,null,1]];" +
          "[[s2,-1.0,null,null,1]];" +
          "[[s3,-1.0,null,null,1]];" +
          "[[s4,-1.0,null,null,1]];" +
          "[[s4,1.0,WrappedArray(100.0, 1000.0),null,1]]"
        // LINEAR
        , "[[s1,-1.0,null,null,1]];" +
          "[[s1,0.25,WrappedArray(25.0, 250.0),null,1]];" +
          "[[s2,-1.0,null,null,1]];" +
          "[[s2,0.25,WrappedArray(25.0, 250.0),null,1]];" +
          "[[s3,-1.0,null,null,1]];" +
          "[[s3,0.25,WrappedArray(25.0, 250.0),null,1]];" +
          "[[s4,-1.0,null,null,1]];" +
          "[[s4,0.25,WrappedArray(25.0, 250.0),null,1]]"
        // POSITION
        , "[[s1,-1.0,null,null,1]];" +
          "[[s1,0.4,WrappedArray(40.0, 400.0),null,1]];" +
          "[[s2,-1.0,null,null,1]];" +
          "[[s2,0.1,WrappedArray(10.0, 100.0),null,1]];" +
          "[[s3,-1.0,null,null,1]];" +
          "[[s3,0.1,WrappedArray(10.0, 100.0),null,1]];" +
          "[[s4,-1.0,null,null,1]];" +
          "[[s4,0.4,WrappedArray(40.0, 400.0),null,1]]"
        // DECAY
        , "[[s1,-1.0,null,null,1]];" +
          "[[s1,0.5,WrappedArray(50.0, 500.0),null,1]];" +
          "[[s2,-1.0,null,null,1]];" +
          "[[s2,0.5,WrappedArray(50.0, 500.0),null,1]];" +
          "[[s3,-1.0,null,null,1]];" +
          "[[s3,0.5,WrappedArray(50.0, 500.0),null,1]];" +
          "[[s4,-1.0,null,null,1]];" +
          "[[s4,1.0,WrappedArray(100.0, 1000.0),null,1]]"
      )

      model.zip(result).foreach { case (model, result) =>
        val df = spark.sql(
          "select explode(at) as event from (select attribution(604810000, ts, " +
            buildSource(Seq(1, 2, 3, 4)) +
            buildTarget(Seq(5)) +
            "null," +
            "'NONE', " +
            "null," +
            "array(measure1, measure2), " +
            s"'$model', " +
            "null) as at " +
            "from events group by uid) order by event.name,event.contrib"
        )
        // df.show(false)
        val actual = df.collect().mkString(";")
        // println(actual)
        assert(result == actual)
      }
    }

    // test measures, grouping infos
    {
      Seq(
        (1, 1, "foo1", "bar1", 0, 0, 1),
        (1, 2, "foo2", "bar2", 0, 0, 2),
        //  (1, 2, "foo2", "bar2", 0, 0, 2),
        (1, 2, "foo3", "bar3", 0, 0, 3),
        (1, 2, "foo3", "bar3", 0, 0, 3),
        //  (1, 2, "foo4", "bar4", 0, 0, 4),
        //  (1, 2, "foo5", "bar5", 0, 0, 5),
        (1, 2, null, null, 0, 0, 5),
        (1, 3, "foo6", "bar6", 100, 1000, 6),

        //  (1, 2, "foo7", "bar7", 0, 0, 7),
        (1, 1, "foo8", "bar8", 0, 0, 8),
        (1, 3, "foo9", "bar9", 100, 1000, 9),

        //  (1, 3, "foo7", "bar7", 100, 1000, 20),
        (1, 3, "foo7", "bar7", 100, 1000, 30),
      ).toDF(colNames: _*).createOrReplaceTempView("events")
      val result =
        "[[d,0.0,null,null,1]];" +
          "[[s1,1.0,WrappedArray(100.0),WrappedArray(foo8),1]];" +
          "[[s1,-1.0,null,WrappedArray(foo1),1]];" +
          "[[s1,-1.0,null,WrappedArray(foo8),1]];" +
          "[[s2,1.0,WrappedArray(100.0),WrappedArray(null),1]];" +
          "[[s2,-1.0,null,WrappedArray(null),1]];" +
          "[[s2,-1.0,null,WrappedArray(foo2),1]];" +
          "[[s2,-1.0,null,WrappedArray(foo3),2]]"

      val df = spark.sql(
        //  "select " +
        //    "event.name," +
        //     "event.groupingInfos[0] by1," +
        //     "event.groupingInfos[1] by2," +
        //     "sum(case when event.contrib = -1 then event.count else 0 end) count_pv_all," +
        //     "sum(case when event.contrib > -1 then event.count else 0 end) count_pv_valid," +
        //     "count(" +
        //      "distinct case when event.contrib = -1 then uid else null end" +
        //     ") count_uv_all," +
        //     "count(" +
        //       "distinct case when event.contrib > -1 then uid else null end" +
        //     ") count_uv_valid," +
        //     "sum(case when event.contrib > -1 then event.contrib else 0 end) sum_contrib," +
        //     "sum(" +
        //      "case when event.contrib > -1 then event.measureContrib[0] else 0 end" +
        //     ") sum_measure_contrib " +
        //      "from ( " +
        "select " +
          //                "uid," +
          "explode(at) as event from (" +
          "select uid,attribution(11, ts, " +
          buildSource(Seq(1, 2)) +
          buildTarget(Seq(3)) +
          "null," +
          "'NONE', " +
          "null," +
          "array(measure1), " +
          "'LAST', " + // "FIRST", "LAST", "LINEAR", "POSITION", "DECAY"
          "array(dim1)" +
          ") as at " +
          "from events group by uid"
          + ") order by event.name,event.count"
        //             + ") group by 1,2,3 order by 1,2,3"
      )
      //          df.show(false)
      val actual = df.collect().mkString(";")
      //          println(actual)
      assert(result == actual)
    }

    // test source relate to target, relation build
    {
      Seq(
        (1, 1, null, "foo", 0, 0, 1),
        (1, 2, "bar", null, 0, 0, 5),
        (1, 3, "bar", null, 0, 0, 6),
        (1, 4, null, "foo", 0, 0, 7),
        (1, 5, "bar", null, 100, 0, 8),
        (1, 4, null, "bar", 0, 0, 9),
        (1, 5, null, "foo", 1000, 0, 10)
      ).toDF(colNames: _*).createOrReplaceTempView("events")
      val result =
        "[[s1,-1.0,null,null,1]];" +
          "[[s2,0.5,WrappedArray(50.0),null,1]];" +
          "[[s2,-1.0,null,null,1]];" +
          "[[s3,0.5,WrappedArray(50.0),null,1]];" +
          "[[s3,-1.0,null,null,1]];" +
          "[[s4,1.0,WrappedArray(1000.0),null,1]];" +
          "[[s4,-1.0,null,null,2]]"
      val sql =
        "select explode(at) as event " +
          "from (" +
          "select " +
          "attribution(" +
          "7, ts, " +
          "case " +
          "when eid=1 then 's1' " +
          "when eid=2 then 's2' " +
          "when eid=3 then 's3' " +
          "when eid=4 then 's4'else null end  ," +
          "case when eid=5 then 't5'else null end  ," +
          "null," +
          "'TARGET_TO_SOURCE', " +
          "array(" +
          "map('t5', dim1, 's2', dim1)," +
          "map('t5', dim1, 's3', dim1)," +
          "map('t5', dim2, 's1', dim2)," +
          "map('t5', dim2, 's4', dim2)" +
          ")," +
          "array(measure1), " +
          "'LINEAR', " +
          "null) as at from events group by uid  " +
          ") order by event.name,event.count"
      val df = spark.sql(sql)
      val actual = df.collect().mkString(";")
      assert(result == actual)
    }

    // test ahead
    {
      Seq(
        (1, 1, null, "foo", 0, 0, 4),
        (1, 2, "bar", null, 0, 0, 5),
        (1, 0, "bar", "foo1", 0, 0, 6),
        (1, 3, "bar", null, 0, 0, 7),
        (1, 4, null, null, 0, 0, 8),
        (1, 0, "bar", "foo", 0, 0, 9),
        (1, 4, null, null, 0, 0, 10),
        (1, 5, null, "foo", 100, 0, 11)
      ).toDF(colNames: _*).createOrReplaceTempView("events")
      val result =
        "[[s1,-1.0,null,null,1]];" +
          "[[s2,-1.0,null,null,1]];" +
          "[[s3,-1.0,null,null,1]];" +
          "[[s4,1.0,WrappedArray(100.0),null,1]];" +
          "[[s4,-1.0,null,null,2]]"
      // ahead relate to target
      val sql = "select explode(at) as event from (" +
        "select " +
        "attribution(" +
        "7, ts, " +
        "case " +
        "when eid=1 then 's1' " +
        "when eid=2 then 's2' " +
        "when eid=3 then 's3' " +
        "when eid=4 then 's4' else null end  ," +
        "case when eid=5 then 't5' else null end  ," +
        "case when eid=0 then 'a0' else null end  ," +
        "'TARGET_TO_AHEAD', " +
        "array(map('t5', dim2, 'a0', dim2))," +
        "array(measure1), 'LINEAR', null) as at " +
        "from events group by uid) order by event.name"
      val df = spark.sql(sql)
      //   df.show(false)
      val actual = df.collect().mkString(";")
      //  println(actual)
      assert(result == actual)
    }
    // ahead relate to source and target
    {
      Seq(
        (1, 1, null, "foo", 0, 0, 1),
        (1, 2, "bar", null, 0, 0, 2),
        (1, 1, "bar", "foo", 0, 0, 3),
        (1, 3, "bar1", null, 0, 0, 4),
        (1, 5, "bar1", "foo", 0, 0, 5), // ahead
        (1, 4, "bar", null, 0, 0, 6),
        (1, 6, "bar1", "foo", 0, 0, 7), // ahead
        (1, 5, "bar", "foo", 0, 0, 8), // ahead
        (1, 6, "bar", "foo", 0, 0, 9), // ahead
        (1, 7, null, "foo", 100, 0, 10),
        (1, 2, "bar1", null, 0, 0, 11),
        (1, 6, "bar1", "foo", 0, 0, 12), // ahead
        (1, 1, "bar1", "foo", 0, 0, 13),
        (1, 5, "bar1", "foo", 0, 0, 14), // ahead
        (1, 7, null, "foo", 100, 0, 15)
      ).toDF(colNames: _*).createOrReplaceTempView("events")
      val result =
        "[[s1,-1.0,null,null,3]];" +
          "[[s2,1.0,WrappedArray(100.0),null,1]];" +
          "[[s2,-1.0,null,null,2]];" +
          "[[s3,-1.0,null,null,1]];" +
          "[[s4,1.0,WrappedArray(100.0),null,1]];" +
          "[[s4,-1.0,null,null,1]]"
      val sql = "select explode(at) as event from (" +
        "select " +
        "attribution(100, ts, " +
        "case " +
        "when eid=1 then 's1' " +
        "when eid=2 then 's2' " +
        "when eid=3 then 's3' " +
        "when eid=4 then 's4'else null end  ," +
        "case when eid=7 then 't7'else null end  ," +
        "case " +
        "when eid=5 then 'a5'" +
        "when eid=6 then 'a6' else null end  ," +
        "'TARGET_TO_AHEAD_TO_SOURCE', " +
        "array(" +
        "map('t7', dim2, 'a5', dim2)," +
        "map('t7', dim2, 'a6', dim2)," +
        "map('a5', dim1, 's1', dim1)," +
        "map('a5', dim1, 's2', dim1)," +
        "map('a5', dim1, 's3', dim1)," +
        "map('a5', dim1, 's4', dim1)," +
        "map('a6', dim1, 's1', dim1)," +
        "map('a6', dim1, 's2', dim1)," +
        "map('a6', dim1, 's3', dim1)," +
        "map('a6', dim1, 's4', dim1)" +
        ")," +
        "array(measure1), " +
        "'LAST', null) as at " + // "FIRST", "LAST", "LINEAR", "POSITION", "DECAY"
        "from events group by uid) " +
        "order by event.name"
      val df = spark.sql(sql)
      val actual = df.collect().mkString(";")
      //    println(actual)
      assert(result == actual)
    }
    {
      Seq(
        (1, 1, null, "foo", 0, 0, 1),
        (1, 2, "bar", null, 0, 0, 2),
        (1, 1, "bar", "foo", 0, 0, 3),
        (1, 3, "bar1", null, 0, 0, 4),
        (1, 5, "bar1", "foo", 0, 0, 5), // ahead
        (1, 4, "bar", null, 0, 0, 6),
        (1, 6, "bar1", "foo", 0, 0, 7), // ahead
        (1, 5, "bar", "foo", 0, 0, 8), // ahead
        (1, 6, "bar", "foo", 0, 0, 9), // ahead
        (1, 7, null, "foo", 100, 0, 10),
        (1, 2, "bar1", null, 0, 0, 11),
        (1, 6, "bar1", "foo", 0, 0, 12), // ahead
        (1, 1, "bar1", "foo", 0, 0, 13),
        (1, 5, "bar1", "foo", 0, 0, 14), // ahead
        (1, 7, null, "foo", 100, 0, 15),
        (1, 7, "bar7", "foo", 100, 0, 1000) // Direct conversion
      ).toDF(colNames: _*).createOrReplaceTempView("events")
      val result = "[d,null,null,0,1,0,1,0.0,null];" +
        "[s1,bar,foo,1,0,1,0,0.0,0.0];" +
        "[s1,bar1,foo,1,0,1,0,0.0,0.0];" +
        "[s1,null,foo,1,0,1,0,0.0,0.0];" +
        "[s2,bar1,null,1,1,1,1,1.0,100.0];" +
        "[s2,bar,null,1,0,1,0,0.0,0.0];" +
        "[s3,bar1,null,1,0,1,0,0.0,0.0];" +
        "[s4,bar,null,1,1,1,1,1.0,100.0]"
      val sql = "select \n" +
        "event.name, --  s1 s2 s3 s4 d, d -> Direct conversion \n" +
        "event.groupingInfos[0] by1, -- maybe null\n" +
        "event.groupingInfos[1] by2,-- maybe null\n" +
        "sum(case when event.contrib = -1 then event.count else 0 end) count_pv_all, \n" +
        "sum(\n" +
        "case when event.contrib > -1 then event.count else 0 end \n" +
        ") count_pv_valid,  \n" +
        "count(\n" +
        "distinct case when event.contrib = -1 then uid else null end\n" +
        ") count_uv_all,  \n" +
        "count(\n" +
        "distinct case when event.contrib > -1 then uid else null end\n" +
        ") count_uv_valid, \n" +
        "sum(case when event.contrib > -1 then event.contrib else 0 end) sum_contrib,  \n" +
        "sum(\n" +
        "case when event.contrib > -1 then event.measureContrib[0] else 0 end \n" +
        ") sum_measure_contrib -- maybe null \n" +
        "from ( \n" +
        "select uid, explode(at) as event from (\n" +
        "select \n" +
        "uid, \n" +
        "attribution(\n" +
        "100, \n" +
        "ts, \n" +
        "case \n" +
        "when eid=1 then 's1' \n" +
        "when eid=2 then 's2' \n" +
        "when eid=3 then 's3' \n" +
        "when eid=4 then 's4' else null end  , \n" +
        "case when eid=7 then 't7' else null end  ,  \n" +
        "case \n" +
        "when eid=5 then 'a5'\n" +
        "when eid=6 then 'a6' else null end  , -- must start with a \n" +
        "'TARGET_TO_AHEAD_TO_SOURCE',  -- NONE AHEAD_ONLY TARGET_TO_SOURCE TARGET_TO_AHEAD\n" +
        "array(  \n" +
        "map('t7', dim2, 'a5', dim2),\n" +
        "map('t7', dim2, 'a6', dim2),\n" +
        "map('a5', dim1, 's1', dim1),\n" +
        "map('a5', dim1, 's2', dim1),\n" +
        "map('a5', dim1, 's3', dim1),\n" +
        "map('a5', dim1, 's4', dim1),\n" +
        "map('a6', dim1, 's1', dim1),\n" +
        "map('a6', dim1, 's2', dim1),\n" +
        "map('a6', dim1, 's3', dim1),\n" +
        "map('a6', dim1, 's4', dim1)\n" +
        "),\n" +
        "array(measure1),   \n" +
        "'LAST', -- FIRST, LAST, LINEAR, POSITION, DECAY\n" +
        "array(dim1,dim2) \n" +
        ") as at \n" +
        "from events group by uid) \n" +
        ") group by 1,2,3 order by name,count_pv_all"
      //      println(sql)
      val df = spark.sql(sql)
      // df.show(false)
      val actual = df.collect().mkString(";")
      //       println(actual)
      assert(result == actual)
    }
  }

  private def buildSource(eid: Seq[Int], comma: Boolean = true): String = {
    buildCaseWhen(eid, "s", comma)
  }

  private def buildTarget(eid: Seq[Int], comma: Boolean = true): String = {
    buildCaseWhen(eid, "t", comma)
  }

  private def buildCaseWhen(eid: Seq[Int], prefix: String, comma: Boolean = true): String = {
    val casewhen = "case " +
      eid.map { i => s"when eid=$i then '$prefix$i'"}.mkString(" ") +
      "else null end "
    if (comma) {
      s"$casewhen ,"
    } else {
      casewhen
    }
  }


  test("single window funnel test") {
    {
      val df = spark.sql(
        """
          with tmp0 as (
            select * from values
            (1, 0, 1, 'a1'),
            (1, 0, 2, 'a2'),
            (1, 0, 3, null),
            (1, 1, 4, 'a4'),
            (1, 2, 5, 'a5'),
            (1, 3, 6, 'a6')
            AS test(user_id,event_id,event_time,dim)
          ),
          tmp1 as (
            select user_id, window_funnel(
              10,-- window
              4,
              'SIMPLE',
              event_time,
              tmp0.dim,
              case
              when event_id = 1 then '1'
              when event_id = 2 then '2'
              when event_id = 3 then '3'
              else '-1' end,
              struct(),
              struct()
            ) seq
            from tmp0
            group by user_id
          )
          select user_id,seq['max_step'] max_step ,seq['0dim'] 0dim
          from tmp1
          """.stripMargin
      )
                  df.show(false)
//      val actual = df.collect().mkString(";")
      //            println(actual)
    }
  }
  test("test window funnel") {
    // simple
    {
      val result = "[1,3,null]"
      val df = spark.sql(
        """
          with tmp0 as (
            select * from values
            (1, 0, 1, 'a1'),
            (1, 0, 2, 'a2'),
            (1, 0, 3, null),
            (1, 1, 4, 'a4'),
            (1, 2, 5, 'a5'),
            (1, 3, 6, 'a6')
            AS test(user_id,event_id,event_time,dim)
          ),
          tmp1 as (
            select user_id, window_funnel(
              10,-- window
              4,
              'SIMPLE',
              event_time,
              tmp0.dim,
              case
              when event_id = 0 then '0'
              when event_id = 1 then '1'
              when event_id = 2 then '2'
              when event_id = 3 then '3'
              else '-1' end,
              struct(),
              struct()
            ) seq
            from tmp0
            group by user_id
          )
          select user_id,seq['max_step'] max_step ,seq['0dim'] 0dim
          from tmp1
          """.stripMargin
      )
      //            df.show(false)
      val actual = df.collect().mkString(";")
      //            println(actual)
      assert(result == actual)
    }
    {
      val result = "[1,3,a3,a4,a5,a8]"
      val df = spark.sql(
        """
        with tmp0 as (
          select * from values
          (1, 0, 1, 'a1'),
          (1, 1, 2, 'a2'),
          (1, 0, 3, 'a3'),
          (1, 1, 4, 'a4'),
          (1, 2, 5, 'a5'),
          (1, 1, 6, 'a6'),
          (1, 2, 7, 'a7'),
          (1, 3, 8, 'a8')
          AS test(user_id,event_id,event_time,dim)
        ),
        tmp1 as (
          select user_id, window_funnel(
            10,-- window
            4,
            'SIMPLE',
            event_time,
            tmp0.dim,
            case
            when event_id = 0 then '0'
            when event_id = 1 then '1'
            when event_id = 2 then '2'
            when event_id = 3 then '3'
            else '-1' end,
            struct(),
            struct(struct(0, dim),struct(1, dim),struct(2, dim),struct(3, dim))
          ) seq
          from tmp0
          group by user_id
        )
        select user_id,seq['max_step'] max_step ,seq['0dim'] 0dim,
        seq['1dim'] 1dim, seq['2dim'] 2dim, seq['3dim'] 3dim
        from tmp1
        """.stripMargin
      )
      //      df.show(false)
      val actual = df.collect().mkString(";")
      //            println(actual)
      assert(result == actual)
    }
    {
      val result = "[1,3,a6,a7,a8,a9]"
      val df = spark.sql(
        """
          with tmp0 as (
            select * from values
            (1, 2, 1, 'a1'),
            (1, 1, 2, 'a2'),
            (1, 2, 3, 'a3'),
            (1, 0, 4, 'a4'),
            (1, 1, 5, 'a5'),
            (1, 0, 6, 'a6'),
            (1, 1, 7, 'a7'),
            (1, 3, 7, 'a7'),
            (1, 2, 8, 'a8'),
            (1, 1, 8, 'a8'),
            (1, 0, 8, 'a8'),
            (1, 2, 8, 'a8'),
            (1, 3, 9, 'a9'),
            (1, 1, 11, 'a11')
            AS test(user_id,event_id,event_time,dim)
          ),
          tmp1 as (
            select user_id, window_funnel(
              6,-- window
              4,
              'SIMPLE',
              event_time,
              tmp0.dim,
              case
              when event_id = 0 then '0'
              when event_id = 1 then '1'
              when event_id = 2 then '2'
              when event_id = 3 then '3'
              else '-1' end,
              struct(),
              struct(struct(1, dim),struct(2, dim),struct(3, dim))
            ) seq
            from tmp0
            group by user_id
          )
          select user_id,seq['max_step'] max_step ,seq['0dim'] 0dim,
          seq['1dim'] 1dim, seq['2dim'] 2dim, seq['3dim'] 3dim
          from tmp1
          """.stripMargin
      )
//      df.show(false)
      val actual = df.collect().mkString(";")
//      println(actual)
      assert(result == actual)
    }

    // SIMPLE_REL
    {
      val result = "[1,3,3,4,5,6]"
      val df = spark.sql(
        """
          with tmp0 as (
            select * from values
            (1, 0, 1, '1', '1', '1', '1'),
            (1, 0, 2, '2', '2', '2', '2'),
            (1, 0, 3, '3', '3', '3', '3'),
            (1, 1, 4, '3', '4', '4', '4'),
            (1, 2, 5, '5', '4', '5', '5'),
            (1, 3, 6, '6', '6', '5', '6')
            AS test(user_id,event_id,event_time,dim1,dim2,dim3,dim4)
          ),
          tmp1 as (
            select user_id, window_funnel(
              10,-- window
              4,
              'SIMPLE_REL',
              event_time,
              tmp0.dim4,
              case
                when event_id = 0 then '0'
                when event_id = 1 then '1'
                when event_id = 2 then '2'
                when event_id = 3 then '3'
              else '-1' end,
              struct(
                struct('NONE',dim1),
                struct(dim1,dim2),
                struct(dim2,dim3),
                struct(dim3,'NONE')
              ),
              struct(struct(1, dim4),struct(2, dim4),struct(3, dim4))
            ) seq
            from tmp0
            group by user_id
          )
          select user_id,seq['max_step'] max_step ,seq['0dim4'] 0dim4,
          seq['1dim4'] 1dim4, seq['2dim4'] 2dim4, seq['3dim4'] 3dim4
          from tmp1
          """.stripMargin
      )
//      df.show(false)
      val actual = df.collect().mkString(";")
//      println(actual)
      assert(result == actual)
    }
    {
      val result = "[1,3,10,21,31,70]"
      val df = spark.sql(
        """
      with tmp0 as (
        select * from values
        (1, 0, 10, '10', '10', '10', '10'),
        (1, 1, 20, '10', '20', '20', '20'),
        (1, 1, 21, '10', '20', '21', '21'),
        (1, 2, 30, '30', '20', '30', '30'),
        (1, 2, 31, '30', '20', '30', '31'),
        (1, 3, 40, '40', '40', '60', '40'),
        (1, 1, 50, '10', '50', '50', '50'),
        (1, 2, 60, '60', '50', '60', '60'),
        (1, 3, 70, '70', '70', '30', '70'),
        (1, 3, 80, '80', '80', '30', '80')
        AS test(user_id,event_id,event_time,dim1,dim2,dim3,dim4)
      ),
      tmp1 as (
        select user_id, window_funnel(
          100,-- window
          4,
          'SIMPLE_REL',
          event_time,
          tmp0.dim4,
          case
            when event_id = 0 then '0'
            when event_id = 1 then '1'
            when event_id = 2 then '2'
            when event_id = 3 then '3'
          else '-1' end,
          struct(
            struct('NONE',dim1),
            struct(dim1,dim2),
            struct(dim2,dim3),
            struct(dim3,'NONE')
          ),
          struct(struct(1, dim4),struct(2, dim4),struct(3, dim4))
        ) seq
        from tmp0
        group by user_id
      )
      select user_id,seq['max_step'] max_step ,seq['0dim4'] 0dim4,
      seq['1dim4'] 1dim4, seq['2dim4'] 2dim4, seq['3dim4'] 3dim4
      from tmp1
      """.stripMargin
      )
      //      df.show(false)
      val actual = df.collect().mkString(";")
      //      println(actual)
      assert(result == actual)
    }
    {
      val result = "[1,3,100,210,310,700]"
      val df = spark.sql(
        """
      with tmp0 as (
        select * from values
        (1, 0, 10, '10', '10', '10', '10'),
        (1, 1, 20, '10', '20', '20', '20'),
        (1, 1, 21, '10', '20', '21', '21'),
        (1, 2, 30, '30', '20', '30', '30'),
        (1, 2, 31, '30', '20', '30', '31'),
        (1, 3, 40, '40', '40', '60', '40'),
        (1, 1, 50, '10', '50', '50', '50'),
        (1, 2, 60, '60', '50', '60', '60'),
        (1, 3, 70, '70', '70', '31', '70'),
        (1, 3, 80, '80', '80', '31', '80'),
        (1, 0, 100, '100', '100', '100', '100'),
        (1, 1, 200, '100', '200', '200', '200'),
        (1, 1, 210, '100', '200', '210', '210'),
        (1, 2, 300, '300', '200', '300', '300'),
        (1, 2, 310, '300', '200', '300', '310'),
        (1, 3, 400, '400', '400', '600', '400'),
        (1, 1, 500, '100', '500', '500', '500'),
        (1, 2, 600, '600', '500', '600', '600'),
        (1, 3, 700, '700', '700', '300', '700'),
        (1, 3, 800, '800', '800', '300', '800')
        AS test(user_id,event_id,event_time,dim1,dim2,dim3,dim4)
      ),
      tmp1 as (
        select user_id, window_funnel(
          1000,-- window
          4,
          'SIMPLE_REL',
          event_time,
          tmp0.dim4,
          case
            when event_id = 0 then '0'
            when event_id = 1 then '1'
            when event_id = 2 then '2'
            when event_id = 3 then '3'
          else '-1' end,
          struct(
            struct('NONE',dim1),
            struct(dim1,dim2),
            struct(dim2,dim3),
            struct(dim3,'NONE')
          ),
          struct(struct(1, dim4),struct(2, dim4),struct(3, dim4))
        ) seq
        from tmp0
        group by user_id
      )
      select user_id,seq['max_step'] max_step ,seq['0dim4'] 0dim4,
      seq['1dim4'] 1dim4, seq['2dim4'] 2dim4, seq['3dim4'] 3dim4
      from tmp1
      """.stripMargin
      )
      //      df.show(false)
      val actual = df.collect().mkString(";")
      //      println(actual)
      assert(result == actual)
    }
    {
      val result = "[1,2,10,21,30,null]"
      val df = spark.sql(
        """
      with tmp0 as (
        select * from values
        (1, 0, 10, '10', '10', '10', '10'),
        (1, 1, 20, '10', '20', '20', '20'),
        (1, 1, 21, '10', '20', '21', '21'),
        (1, 2, 30, '30', '20', '30', '30'),
        (1, 2, 31, '30', '20', '30', '31'),
        (1, 3, 40, '40', '40', '60', '40'),
        (1, 1, 50, '10', '50', '50', '50'),
        (1, 2, 60, '60', '50', '60', '60'),
        (1, 3, 70, '70', '70', '31', '70'),
        (1, 3, 80, '80', '80', '31', '80'),
        (1, 0, 100, '100', '100', '100', '100'),
        (1, 1, 200, '100', '200', '200', '200'),
        (1, 1, 210, '100', '200', '210', '210'),
        (1, 2, 300, '300', '200', '300', '300'),
        (1, 2, 310, '300', '200', '300', '310'),
        (1, 3, 400, '400', '400', '600', '400'),
        (1, 1, 500, '100', '500', '500', '500'),
        (1, 2, 600, '600', '500', '600', '600'),
        (1, 3, 700, '700', '700', '310', '700'),
        (1, 3, 800, '800', '800', '310', '800')
        AS test(user_id,event_id,event_time,dim1,dim2,dim3,dim4)
      ),
      tmp1 as (
        select user_id, window_funnel(
          1000,-- window
          4,
          'SIMPLE_REL',
          event_time,
          tmp0.dim4,
          case
            when event_id = 0 then '0'
            when event_id = 1 then '1'
            when event_id = 2 then '2'
            when event_id = 3 then '3'
          else '-1' end,
          struct(
            struct('NONE',dim1),
            struct(dim1,dim2),
            struct(dim2,dim3),
            struct(dim3,'NONE')
          ),
          struct(struct(1, dim4),struct(2, dim4),struct(3, dim4))
        ) seq
        from tmp0
        group by user_id
      )
      select user_id,seq['max_step'] max_step ,seq['0dim4'] 0dim4,
      seq['1dim4'] 1dim4, seq['2dim4'] 2dim4, seq['3dim4'] 3dim4
      from tmp1
      """.stripMargin
      )
      //      df.show(false)
      val actual = df.collect().mkString(";")
      //      println(actual)
      assert(result == actual)
    }
    // repeat
    {
      val result = "[1,2,a1,a3,a4]"
      val df = spark.sql(
        """
          with tmp0 as (
            select * from values
            (1, 0, 1, 'a1'),
            (1, 0, 3, 'a3'),
            (1, 0, 4, 'a4')
            AS test(user_id,event_id,event_time,dim)
          ),
          tmp1 as (
            select user_id, window_funnel(
              6,
              3,
              'REPEAT',
              event_time,
              tmp0.dim,
              case when event_id = 0 then '0,1,2'  else '-1' end,
              struct(),
              struct(struct(0, dim),struct(1, dim),struct(2, dim))
            ) seq
            from tmp0 group by user_id
          )
          select user_id,seq['max_step'] max_step ,seq['0dim'] 0dim ,
          seq['1dim'] 1dim ,seq['2dim'] 2dim
          from tmp1
        """.stripMargin
      )
      //      df.show(false)
      val actual = df.collect().mkString(";")
      //      println(actual)
      assert(result == actual)
    }
    {
      val result = "[1,2,a5,a8,a9]"
      val df = spark.sql(
        """
          with tmp0 as (
            select * from values
            (1, 0, 1, 'a1'),
          --  (1, 1, 3, 'a3'),
            (1, 0, 4, 'a4'),
            (1, 0, 5, 'a5'),
            (1, 1, 8, 'a8'),
            (1, 0, 9, 'a9')
            AS test(user_id,event_id,event_time,dim)
          ),
          tmp1 as (
            select user_id, window_funnel(
              6,
              3,
              'REPEAT',
              event_time,
              tmp0.dim,
              case
                when event_id = 0 then '0,2'
                when event_id = 1 then '1'
              else '-1' end,
              struct(),
              struct(struct(0, dim),struct(1, dim),struct(2, dim))
            ) seq
            from tmp0 group by user_id
          )
          select user_id,seq['max_step'] max_step ,seq['0dim'] 0dim ,
          seq['1dim'] 1dim ,seq['2dim'] 2dim
          from tmp1
        """.stripMargin
      )
      //      df.show(false)
      val actual = df.collect().mkString(";")
      //      println(actual)
      assert(result == actual)
    }
    // repeat rel
    {
      val result = "[1,3,100,210,310,700]"
      val df = spark.sql(
        """
          with tmp0 as (
            select * from values
            (1, 0, 10, '10', '10', '10', '10'),
            (1, 1, 20, '10', '20', '20', '20'),
            (1, 1, 21, '10', '20', '21', '21'),
            (1, 2, 30, '30', '20', '30', '30'),
            (1, 2, 31, '30', '20', '30', '31'),
            (1, 3, 40, '40', '40', '60', '40'),
            (1, 1, 50, '10', '50', '50', '50'),
            (1, 2, 60, '60', '50', '60', '60'),
            (1, 3, 70, '70', '70', '31', '70'),
            (1, 3, 80, '80', '80', '31', '80'),
            (1, 0, 100, '100', '100', '100', '100'),
            (1, 1, 200, '100', '200', '200', '200'),
            (1, 1, 210, '100', '200', '210', '210'),
            (1, 2, 300, '300', '200', '300', '300'),
            (1, 2, 310, '300', '200', '300', '310'),
            (1, 3, 400, '400', '400', '600', '400'),
            (1, 1, 500, '100', '500', '500', '500'),
            (1, 2, 600, '600', '500', '600', '600'),
            (1, 3, 700, '700', '700', '300', '700'),
            (1, 3, 800, '800', '800', '300', '800')
            AS test(user_id,event_id,event_time,dim1,dim2,dim3,dim4)
          ),
          tmp1 as (
            select user_id, window_funnel(
              1000,-- window
              4,
              'REPEAT_REL',
              event_time,
              tmp0.dim4,
              case
                when event_id = 0 then '0'
                when event_id = 1 then '1'
                when event_id = 2 then '2'
                when event_id = 3 then '3'
              else '-1' end,
              struct(
                struct('NONE',dim1),
                struct(dim1,dim2),
                struct(dim2,dim3),
                struct(dim3,'NONE')
              ),
              struct(struct(1, dim4),struct(2, dim4),struct(3, dim4))
            ) seq
            from tmp0
            group by user_id
          )
          select user_id,seq['max_step'] max_step ,seq['0dim4'] 0dim4,
          seq['1dim4'] 1dim4, seq['2dim4'] 2dim4, seq['3dim4'] 3dim4
          from tmp1
        """.stripMargin
      )
      //      df.show(false)
      val actual = df.collect().mkString(";")
      //      println(actual)
      assert(result == actual)
    }
    {
      val result = "[1,2,a1,b3,c4]"
      val df = spark.sql(
        """
          with tmp0 as (
            select * from values
            (1, 0, 1, 'a1','b1','c1'),
            (1, 1, 3, 'a1','b3','c3'),
            (1, 0, 4, 'a4','b3','c4')
            AS test(user_id,event_id,event_time,dim0,dim1,dim2)
          ),
          tmp1 as (
            select user_id, window_funnel(
              6,
              3,
              'REPEAT_REL',
              event_time,
              tmp0.dim0,
              case when event_id = 0 then '0,2'
               when event_id = 1 then '1'
              else '-1' end,
              struct(struct('NONE',dim0),
                    struct(dim0,dim1),
                    struct(dim1,'NONE')),
              struct(struct(1, dim1),struct(2, dim2))
            ) seq
            from tmp0 group by user_id
          )
          select user_id,seq['max_step'] max_step ,seq['0dim0'] 0dim0 ,
          seq['1dim1'] 1dim1 ,seq['2dim2'] 2dim2
          from tmp1
        """.stripMargin
      )
      //      df.show(false)
      val actual = df.collect().mkString(";")
      //      println(actual)
      assert(result == actual)
    }
    {
      val result = "[1,0,a1,null,null]"
      val df = spark.sql(
        """
          with tmp0 as (
            select * from values
            (1, 0, 1, 'a1','b1','c1'),
            (1, 1, 3, 'a10','b3','c3'),
            (1, 0, 4, 'a4','b3','c4')
            AS test(user_id,event_id,event_time,dim0,dim1,dim2)
          ),
          tmp1 as (
            select user_id, window_funnel(
              6,
              3,
              'REPEAT_REL',
              event_time,
              tmp0.dim0,
              case when event_id = 0 then '0,2'
               when event_id = 1 then '1'
              else '-1' end,
              struct(struct('NONE',dim0),
                    struct(dim0,dim1),
                    struct(dim1,'NONE')),
              struct(struct(1, dim1),struct(2, dim2))
            ) seq
            from tmp0 group by user_id
          )
          select user_id,seq['max_step'] max_step ,seq['0dim0'] 0dim0 ,
          seq['1dim1'] 1dim1 ,seq['2dim2'] 2dim2
          from tmp1
        """.stripMargin
      )
      //      df.show(false)
      val actual = df.collect().mkString(";")
      //      println(actual)
      assert(result == actual)
    }
    {
      val result = "[1,2,a1,b5,c6]"
      val df = spark.sql(
        """
          with tmp0 as (
            select * from values
            (1, 0, 1, 'a1','b1','c1'),
            (1, 1, 3, 'a1','b3','c3'),
            (1, 0, 4, 'a4','b30','c4'),
            (1, 1, 5, 'a1','b5','c5'),
            (1, 0, 6, 'a6','b5','c6')
            AS test(user_id,event_id,event_time,dim0,dim1,dim2)
          ),
          tmp1 as (
            select user_id, window_funnel(
              6,
              3,
              'REPEAT_REL',
              event_time,
              tmp0.dim0,
              case when event_id = 0 then '0,2'
               when event_id = 1 then '1'
              else '-1' end,
              struct(struct('NONE',dim0),
                    struct(dim0,dim1),
                    struct(dim1,'NONE')),
              struct(struct(1, dim1),struct(2, dim2))
            ) seq
            from tmp0 group by user_id
          )
          select user_id,seq['max_step'] max_step ,seq['0dim0'] 0dim0 ,
          seq['1dim1'] 1dim1 ,seq['2dim2'] 2dim2
          from tmp1
        """.stripMargin
      )
      //      df.show(false)
      val actual = df.collect().mkString(";")
      //      println(actual)
      assert(result == actual)
    }
    {
      val result = "[1,3,10,10,20,30,21,20,false,true]"
      val df = spark.sql(
        """
      with tmp0 as (
        select * from values
        (1, 0, 10, '10', '10', '10', '10'),
        (1, 1, 20, '10', '20', '20', '20'),
        (1, 1, 21, '10', '20', '21', '21'),
        (1, 2, 30, '30', '20', '30', '30'),
        (1, 2, 31, '30', '20', '30', '31'),
        (1, 3, 40, '40', '40', '60', '40'),
        (1, 1, 50, '10', '50', '50', '50'),
        (1, 2, 60, '60', '50', '60', '60'),
        (1, 3, 70, '70', '70', '30', '70'),
        (1, 3, 80, '80', '80', '30', '80')
        AS test(user_id,event_id,event_time,dim1,dim2,dim3,dim4)
      ),
      tmp1 as (
        select user_id, window_funnel(
          100,-- window
          4,
          'SIMPLE_REL',
          event_time,
          tmp0.dim4,
          case
            when event_id = 0 then '0'
            when event_id = 1 then '1'
            when event_id = 2 then '2'
            when event_id = 3 then '3'
          else '-1' end,
          struct(
            struct('NONE',dim1),
            struct(dim1,dim2),
            struct(dim2,dim3),
            struct(dim3,'NONE')
          ),
          struct(
            struct(0, dim1),struct(1, dim1),struct(2, dim2),struct(3, dim3)
            ,struct('user', dim4),struct('user', dim2)
            ,struct('user', case when event_time > 50 then 'true' else 'false' end as ug1)
            ,struct('user', case when event_time < 50 then 'true' else 'false' end as ug2)
          )
        ) seq
        from tmp0
        group by user_id
      )
      select user_id,seq['max_step'] max_step ,seq['0dim4'] 0dim4,
      seq['1dim1'] 1dim1, seq['2dim2'] 2dim2, seq['3dim3'] 3dim3,
      seq['userdim4'] userdim4,seq['userdim2'] userdim2,
      seq['userug1'] userug1,seq['userug2'] userug2
      from tmp1
      """.stripMargin
      )
      //            df.show(false)
      val actual = df.collect().mkString(";")
      //            println(actual)
      assert(result == actual)
    }

  }
  test("test compress bitmap build") {
    val colNames = Seq("user_id", "event_id", "event_time", "dim")
    val df1 = Seq(
      (1, 0, -1, "a"),
      (1, 1, 2, "a"),
      (1, 2, 3, "a"),
      (1, 0, 4, "a"),
      (1, 1, 5, "a"),
      (1, 2, 9, "a"),
      (1, 1, 11, "a"),
    ).toDF(colNames: _*)
    df1.createOrReplaceTempView("events")
    val result = spark.sql(
      "select event_id ,compress_bitmap_build(event_time) " +
        "from events group by event_id"
    )
    result.show(false)
  }

  test("test compress bitmap contains") {
    val colNames = Seq("user_id", "event_id", "event_time", "dim")
    val df1 = Seq(
      (1, 0, -1, "a"),
      (1, 1, 2, "a"),
      (1, 2, 3, "a"),
      (1, 0, 4, "a"),
      (1, 1, 5, "a"),
      (1, 2, 9, "a"),
      (1, 1, 11, "a"),
    ).toDF(colNames: _*)
    df1.createOrReplaceTempView("events")
    val result = spark.sql(
      "select compress_bitmap_contains(event_time," +
        "'H4sIAAAAAAAAAGNgYGBgAmIGKwMGBkYGCBAAYhaG/0AAE/3/HyL6/z8ADXNawzEAAAA=') result " +
        "from events "
    )
    result.show(false)

    val result2 = spark.sql(
      "select " +
        "compress_bitmap_contains(event_time," +
        "'H4sIAAAAAAAAAGNgYGBgBGIGKwMog4GJQQBMsjJwMwAAo5b30B8AAAA=') result2 " +
        "from events " +
        "where compress_bitmap_contains(event_time," +
        "'H4sIAAAAAAAAAGNgYGBgBGIGKwMoA0gKAElmBk4GAAWcUMgdAAAA')"
    )
    result2.show(false)

    val result4 = spark.sql(
      "select compress_bitmap_contains(1," +
        "'H4sIAAAAAAAAAGNgYGBgBGIGKwMoA0gKAElmBk4GAAWcUMgdAAAA') result4 " +
        "from events "
    )
    result4.show(false)
  }

  test("test compress get bitmap") {
    val colNames = Seq("user_id", "event_id", "event_time", "dim")
    val df1 = Seq(
      (1, 0, -1, "a"),
      (1, 1, 2, "a"),
      (1, 2, 3, "a"),
      (1, 0, 4, "a"),
      (1, 1, 5, "a"),
      (1, 2, 9, "a"),
      (1, 1, 11, "a"),
    ).toDF(colNames: _*)
    df1.createOrReplaceTempView("events")
    val result = spark.sql(
      "select  compress_get_bitmap(" +
        "'com.mysql.jdbc.Driver','jdbc:mysql://localhost:3306/test','root'," +
        "'xxxxx','select name from test where id =1') " +
        "from events "
    )
    result.show(false)
  }
  test("single interval test") {
    {
      val result = "[2023-02-13,a1,22];" +
        "[2023-02-13,a3,766];" +
        "[2023-02-13,a6,2000];" +
        "[2023-02-13,a11,1000]"
      val df = spark.sql(
        """
      with tmp0 as (
        select user_id,event_id,
        cast(event_time as timestamp),
        dim1,dim2,dim3
         from values
        (1, 1, '2023-02-14 00:00:00.123', 'a1','b1','c1'),
        (1, 0, '2023-02-15 00:00:00.123', 'a1','b1','c1'),
        (1, 1, '2023-02-15 00:00:00.145', 'a2','b2','b1'),
        (1, 0, '2023-02-15 00:00:01.234', 'a3','b3','b1'),
        (1, 2, '2023-02-15 00:00:02',     'a4','c1','b3'),
        (1, 1, '2023-02-15 00:00:03.123', 'a5',null,'c1'),
        (1, 2, '2023-02-15 00:00:05.123', 'a6','b4',null),
        (1, 1, '2023-02-15 00:00:06.123', 'a7','b5',null),
        (1, 1, '2023-02-15 00:00:07.123', 'a8',null,'b4'),
        (1, 0, '2023-02-15 00:00:08.123', 'a9',null,null),
        (1, 1, '2023-02-15 00:00:09.123', 'a10',null,'b4'),
        (1, 2, '2023-02-15 00:00:10.123', 'a11','b5',null),
        (1, 1, '2023-02-15 00:00:11.123', 'a12',null,'b5')
        AS test(user_id,event_id,event_time,dim1,dim2,dim3)
      ),
      tmp1 as (
        select user_id, interval(
          case
          when event_id = 0 or event_id = 2 then 0
          else -1 end,
          case
          when event_id = 2 or event_id = 1 then 1
          else -1 end,
          event_time,
          'NOT_REPEAT_VIRTUAL_REL',
          array(dim2,dim3),
          dim1,
          'START',
          'WEEK'
        ) seq
        from tmp0 group by user_id
      ),
        tmp2 as(
        select user_id,explode(seq) as event
        from tmp1
        )
        select
         event.agg_date,event.group_info,event.interval_ms
        from tmp2
        where event.interval_ms is not null
    """.stripMargin
      )
      df.show(false)
//      val actual = df.collect().mkString(";")
//                        println(actual)
//      assert(result == actual)
    }
  }
  test("interval test") {
    {
      val result = "[1,[2023-02-13,a2,877]];[1,[2023-02-20,a6,262809123]]"
      val df = spark.sql(
        """
      with tmp0 as (
        select user_id,event_id,
        cast(event_time as timestamp),
        dim1,dim2,dim3
         from values
        (1, 1, '2023-02-14 00:00:00.123', 'a1','b1','c1'),
        (1, 0, '2023-02-15 00:00:00.123', 'a2','b1','c1'),
        (1, 1, '2023-02-15 00:00:01',     'a3','b1','c1'),
        (1, 0, '2023-02-15 00:00:01',     'a4','b1','c1'),
        (1, 1, '2023-02-20 00:00:05.123', 'a5','b1','c1'),
        (1, 0, '2023-02-21 00:00:09',     'a6','b1','c1'),
        (1, 1, '2023-02-21 00:00:18.123', 'a7','b1','c2'),
        (1, 0, '2023-02-21 01:00:09',     'a8','b1','c1'),
        (1, 1, '2023-02-24 01:00:18.123', 'a9','b1','c1')
        AS test(user_id,event_id,event_time,dim1,dim2,dim3)
      ),
      tmp1 as (
        select user_id, interval(
          case
          when event_id = 0 and dim2 ='b1' then 0
          when event_id = 1 and dim3 ='c1' then 1
          else -1 end,
          null,
          event_time,
          'SIMPLE',
          null,
          dim1,
          'START',
          'WEEK'
        ) seq
        from tmp0 group by user_id
      )
      select user_id,explode(seq) as event
      from tmp1
    """.stripMargin
      )
//                  df.show(false)
      val actual = df.collect().mkString(";")
//                  println(actual)
      assert(result == actual)
    }
    {
      val result = "[1,[2023-02-13,a2,877]];[1,[2023-02-20,a6,20123]];[1,[2023-02-20,a8,10123]]"
      val df = spark.sql(
        """
      with tmp0 as (
        select user_id,event_id,
        cast(event_time as timestamp),
        dim1,dim2,dim3
         from values
        (1, 1, '2023-02-14 00:00:00.123', 'a1','b1','c1'),
        (1, 0, '2023-02-15 00:00:00.123', 'a2','b1','c1'),
        (1, 1, '2023-02-15 00:00:01',     'a3','b1','b1'),
        (1, 0, '2023-02-15 00:00:01',     'a4','c1','c1'),
        (1, 1, '2023-02-20 00:00:05.123', 'a5','b1','c1'),
        (1, 0, '2023-02-21 00:00:09',     'a6','b1','c1'),
        (1, 1, '2023-02-21 00:00:18.123', 'a7','b1','bb1'),
        (1, 0, '2023-02-21 00:00:20',     'a8','c1','c1'),
        (1, 1, '2023-02-21 00:00:29.123', 'a9','b1','b1'),
        (1, 0, '2023-02-21 00:00:30',     'a8',null,'c1'),
        (1, 1, '2023-02-21 00:00:30.123', 'a8',null,'c1'),
        (1, 1, '2023-02-21 00:00:31.123', 'a9','b1','b1'),
        (1, 0, '2023-02-21 00:00:32',     'a8','c1','c1'),
        (1, 1, '2023-02-21 00:00:33.123', 'a9','b1',null)
        AS test(user_id,event_id,event_time,dim1,dim2,dim3)
      ),
      tmp1 as (
        select user_id, interval(
          case
          when event_id = 0 then 0
          when event_id = 1 then 1
          else -1 end,
          null,
          event_time,
          'SIMPLE_REL',
          array(dim2,dim3),
          dim1,
          'START',
          'WEEK'
        ) seq
        from tmp0 group by user_id
      )
      select user_id,explode(seq) as event
      from tmp1
    """.stripMargin
      )
      //                        df.show(false)
      val actual = df.collect().mkString(";")
      //                        println(actual)
      assert(result == actual)
    }
    {
      val result = "[1,[2023-02-13,a1,22]];[1,[2023-02-13,a3,766]];[1,[2023-02-13,a5,28000]]"
      val df = spark.sql(
        """
      with tmp0 as (
        select user_id,event_id,
        cast(event_time as timestamp),
        dim1,dim2,dim3
         from values
        (1, 0, '2023-02-15 00:00:00.123', 'a1','b1','c1'),
        (1, 0, '2023-02-15 00:00:00.145', 'a2','b1','c1'),
        (1, 0, '2023-02-15 00:00:01.234',     'a3','b1','b1'),
        (1, 1, '2023-02-15 00:00:01.456',     'a3','b1','b1'),
        (1, 0, '2023-02-15 00:00:02',     'a4','c1','c1'),
        (1, 0, '2023-02-15 00:00:05.123', 'a5','b1','c1'),
        (1, 0, '2023-02-15 00:00:33.123', 'a9','b1',null)
        AS test(user_id,event_id,event_time,dim1,dim2,dim3)
      ),
      tmp1 as (
        select user_id, interval(
          case
          when event_id = 0 then 2
          else -1 end,
          null,
          event_time,
          'REPEAT',
          null,
          dim1,
          'START',
          'WEEK'
        ) seq
        from tmp0 group by user_id
      )
      select user_id,explode(seq) as event
      from tmp1
    """.stripMargin
      )
      //                        df.show(false)
      val actual = df.collect().mkString(";")
      //                        println(actual)
      assert(result == actual)
    }
    {
      val result = "[1,[2023-02-13,a1,1111]];[1,[2023-02-13,a2,1855]];[1,[2023-02-13,a6,2000]]"
      val df = spark.sql(
        """
      with tmp0 as (
        select user_id,event_id,
        cast(event_time as timestamp),
        dim1,dim2,dim3
         from values
        (1, 0, '2023-02-15 00:00:00.123', 'a1','b1','c1'),
        (1, 0, '2023-02-15 00:00:00.145', 'a2','b2','c1'),
        (1, 0, '2023-02-15 00:00:01.234', 'a3','b3','b1'),
        (1, 0, '2023-02-15 00:00:02',     'a4','c1','b2'),
        (1, 0, '2023-02-15 00:00:03.123', 'a5',null,'c1'),
        (1, 0, '2023-02-15 00:00:05.123', 'a6','b4',null),
        (1, 0, '2023-02-15 00:00:06.123', 'a7','b5',null),
        (1, 0, '2023-02-15 00:00:07.123', 'a8',null,'b4')
        AS test(user_id,event_id,event_time,dim1,dim2,dim3)
      ),
      tmp1 as (
        select user_id, interval(
          case
          when event_id = 0 then 2
          else -1 end,
          null,
          event_time,
          'REPEAT_REL',
          array(dim2,dim3),
          dim1,
          'START',
          'WEEK'
        ) seq
        from tmp0 group by user_id
      )
      select user_id,explode(seq) as event
      from tmp1
    """.stripMargin
      )
      //                        df.show(false)
      val actual = df.collect().mkString(";")
      //                        println(actual)
      assert(result == actual)
    }
    {
      val result = "[1,[2023-02-13,a1,22]];[1,[2023-02-13,a3,766]];[1,[2023-02-13,a6,1000]]"
      val df = spark.sql(
        """
      with tmp0 as (
        select user_id,event_id,
        cast(event_time as timestamp),
        dim1,dim2,dim3
         from values
        (1, 1, '2023-02-14 00:00:00.123', 'a1','b1','c1'),
        (1, 0, '2023-02-15 00:00:00.123', 'a1','b1','c1'),
        (1, 1, '2023-02-15 00:00:00.145', 'a2','b2','c1'),
        (1, 0, '2023-02-15 00:00:01.234', 'a3','b3','b1'),
        (1, 2, '2023-02-15 00:00:02',     'a4','c1','b2'),
        (1, 1, '2023-02-15 00:00:03.123', 'a5',null,'c1'),
        (1, 2, '2023-02-15 00:00:05.123', 'a6','b4',null),
        (1, 1, '2023-02-15 00:00:06.123', 'a7','b5',null),
        (1, 0, '2023-02-15 00:00:07.123', 'a8',null,'b4')
        AS test(user_id,event_id,event_time,dim1,dim2,dim3)
      ),
      tmp1 as (
        select user_id, interval(
          case
          when event_id = 0 or event_id = 2 then 0
          else -1 end,
          case
          when event_id = 2 or event_id = 1 then 1
          else -1 end,
          event_time,
          'NOT_REPEAT_VIRTUAL',
          array(dim2,dim3),
          dim1,
          'START',
          'WEEK'
        ) seq
        from tmp0 group by user_id
      )
      select user_id,explode(seq) as event
      from tmp1
    """.stripMargin
      )
      //                        df.show(false)
      val actual = df.collect().mkString(";")
      //                        println(actual)
      assert(result == actual)
    }
    {
      val result = "[1,[2023-02-13,a1,22]];[1,[2023-02-13,a3,766]];" +
        "[1,[2023-02-13,a6,2000]];[1,[2023-02-13,a11,1000]]"
      val df = spark.sql(
        """
      with tmp0 as (
        select user_id,event_id,
        cast(event_time as timestamp),
        dim1,dim2,dim3
         from values
        (1, 1, '2023-02-14 00:00:00.123', 'a1','b1','c1'),
        (1, 0, '2023-02-15 00:00:00.123', 'a1','b1','c1'),
        (1, 1, '2023-02-15 00:00:00.145', 'a2','b2','b1'),
        (1, 0, '2023-02-15 00:00:01.234', 'a3','b3','b1'),
        (1, 2, '2023-02-15 00:00:02',     'a4','c1','b3'),
        (1, 1, '2023-02-15 00:00:03.123', 'a5',null,'c1'),
        (1, 2, '2023-02-15 00:00:05.123', 'a6','b4',null),
        (1, 1, '2023-02-15 00:00:06.123', 'a7','b5',null),
        (1, 1, '2023-02-15 00:00:07.123', 'a8',null,'b4'),
        (1, 0, '2023-02-15 00:00:08.123', 'a9',null,null),
        (1, 1, '2023-02-15 00:00:09.123', 'a10',null,'b4'),
        (1, 2, '2023-02-15 00:00:10.123', 'a11','b5',null),
        (1, 1, '2023-02-15 00:00:11.123', 'a12',null,'b5')
        AS test(user_id,event_id,event_time,dim1,dim2,dim3)
      ),
      tmp1 as (
        select user_id, interval(
          case
          when event_id = 0 or event_id = 2 then 0
          else -1 end,
          case
          when event_id = 2 or event_id = 1 then 1
          else -1 end,
          event_time,
          'NOT_REPEAT_VIRTUAL_REL',
          array(dim2,dim3),
          dim1,
          'START',
          'WEEK'
        ) seq
        from tmp0 group by user_id
      )
      select user_id,explode(seq) as event
      from tmp1
    """.stripMargin
      )
      //                        df.show(false)
      val actual = df.collect().mkString(";")
      //                        println(actual)
      assert(result == actual)
    }
    {
      val result = "[2023-02-13,a1,22];" +
        "[2023-02-13,a3,766];" +
        "[2023-02-13,a6,2000];" +
        "[2023-02-13,a11,1000]"
      val df = spark.sql(
        """
      with tmp0 as (
        select user_id,event_id,
        cast(event_time as timestamp),
        dim1,dim2,dim3
         from values
        (1, 1, '2023-02-14 00:00:00.123', 'a1','b1','c1'),
        (1, 0, '2023-02-15 00:00:00.123', 'a1','b1','c1'),
        (1, 1, '2023-02-15 00:00:00.145', 'a2','b2','b1'),
        (1, 0, '2023-02-15 00:00:01.234', 'a3','b3','b1'),
        (1, 2, '2023-02-15 00:00:02',     'a4','c1','b3'),
        (1, 1, '2023-02-15 00:00:03.123', 'a5',null,'c1'),
        (1, 2, '2023-02-15 00:00:05.123', 'a6','b4',null),
        (1, 1, '2023-02-15 00:00:06.123', 'a7','b5',null),
        (1, 1, '2023-02-15 00:00:07.123', 'a8',null,'b4'),
        (1, 0, '2023-02-15 00:00:08.123', 'a9',null,null),
        (1, 1, '2023-02-15 00:00:09.123', 'a10',null,'b4'),
        (1, 2, '2023-02-15 00:00:10.123', 'a11','b5',null),
        (1, 1, '2023-02-15 00:00:11.123', 'a12',null,'b5')
        AS test(user_id,event_id,event_time,dim1,dim2,dim3)
      ),
      tmp1 as (
        select user_id, interval(
          case
          when event_id = 0 or event_id = 2 then 0
          else -1 end,
          case
          when event_id = 2 or event_id = 1 then 1
          else -1 end,
          event_time,
          'NOT_REPEAT_VIRTUAL_REL',
          array(dim2,dim3),
          dim1,
          'START',
          'WEEK'
        ) seq
        from tmp0 group by user_id
      ),
        tmp2 as(
        select user_id,explode(seq) as event
        from tmp1
        )
        select
         event.agg_date,event.group_info,event.interval_ms
        from tmp2
    """.stripMargin
      )
      //                        df.show(false)
      val actual = df.collect().mkString(";")
      //                        println(actual)
      assert(result == actual)
    }
  }
  test("single user path test") {
    {
      val result = "[2023-02-13,a1,22];" +
        "[2023-02-13,a3,766];" +
        "[2023-02-13,a6,2000];" +
        "[2023-02-13,a11,1000]"
      val df = spark.sql(
        """
      with tmp0 as (
        select user_id,event_id,
        cast(event_time as timestamp),
        dim1,dim2,sid
         from values
        (1, 1, '2023-02-15 00:00:01', 'a1','b1','s1'),
        (1, 2, '2023-02-15 00:00:02', 'a1','b1','s1'),
        (1, 3, '2023-02-15 00:00:03', 'a2','b2','s1'),
        (1, 4, '2023-02-15 00:00:04', 'a3','b3','s1'),
        (1, 5, '2023-02-15 00:00:05', 'a1','b1','s1'),

        (1, 1, '2023-02-15 00:00:10', 'a1','b1','s2'),
        (1, 3, '2023-02-15 00:00:11', 'a2','b2','s2'),
        (1, 3, '2023-02-15 00:00:12', 'a3','b3','s2'),
        (1, 1, '2023-02-15 00:00:13', 'a1','b1','s2'),
        (1, 4, '2023-02-15 00:00:14', 'a1','b1','s2'),

        (1, 1, '2023-02-15 00:00:21', 'a2','b2','s3'),
        (1, 4, '2023-02-15 00:00:22', 'a3','b3','s3'),
        (1, 1, '2023-02-15 00:00:23', 'a1','b1','s3'),
        (1, 2, '2023-02-15 00:00:24', 'a1','b1','s3'),
        (1, 5, '2023-02-15 00:00:25', 'a2','b2','s3'),

        (1, 1, '2023-02-15 00:00:36', 'a3','b3','s4'),
        (1, 5, '2023-02-15 00:00:37', 'a3','b3','s4'),
        (1, 2, '2023-02-15 00:00:38', 'a3','b3','s4'),
        (1, 4, '2023-02-15 00:00:39', 'a3','b3','s4'),
        (1, 3, '2023-02-15 00:00:40', 'a3','b3','s4'),

        (1, 1, '2023-02-15 00:00:51', 'a3','b3','s5'),
        (1, 1, '2023-02-15 00:00:52', 'a3','b3','s5'),
        (1, 4, '2023-02-15 00:00:53', 'a3','b3','s5'),
        (1, 5, '2023-02-15 00:00:54', 'a3','b3','s5'),
        (1, 3, '2023-02-15 00:00:55', 'a3','b3','s5'),

        (2, 3, '2023-02-15 00:00:26', 'a3','b3','s6'),
        (2, 2, '2023-02-15 00:00:27', 'a3','b3','s6'),
        (2, 5, '2023-02-15 00:00:28', 'a3','b3','s6')
        AS test(user_id,event_id,event_time,dim1,dim2,sid)
      ),
      tmp1 as (
        select user_id, user_path_analysis(
          event_time,
          case
          when event_id = 1  then 1
          when event_id = 2  then 2
          when event_id = 3  then 3
          when event_id = 4  then 4
          when event_id = 5  then 5
          else -1 end,
          case when event_id = 2  then dim1 else null end,
          'START',
          1,
          true,
          sid,
          8000
        ) seq
        from tmp0 group by user_id
      ),
      tmp2 as(
      select user_id,explode(seq) as up
      from tmp1
      ),
      tmp3 as(
      select
       user_id,(user_id || '-' || up.sid) as sid,up.seqNum,up.eid,up.eidBy,up.nextEid,up.nextEidBy
      from tmp2
      )
      -- select * from tmp3 order by sid asc , seqNum asc
      select seqNum , eid, eidBy ,nextEid ,nextEidBy,
      count(distinct sid) sid_cnt,
      compress_bitmap_build(user_id) bm
      from tmp3
      group by 1,2,3,4,5
      order by 1 asc,6 desc
    """.stripMargin
      )
      /*

       */

      df.show(100, false)
      //      val actual = df.collect().mkString(";")
      //                        println(actual)
      //      assert(result == actual)
    }
  }


  test("user path test") {
    {
      val df = spark.sql(
        """
      with tmp0 as (
        select user_id,event_id,
        cast(event_time as timestamp),
        dim1,dim2,sid
         from values
        (1, 1, '2023-02-15 00:00:00.01', 'a1','b1','s1'),
        (1, 2, '2023-02-15 00:00:00.02', 'a1','b1','s1'),
        (1, 3, '2023-02-15 00:00:00.03', 'a2','b2','s1'),
        (1, 4, '2023-02-15 00:00:00.04', 'a3','b3','s1'),
        (1, 5, '2023-02-15 00:00:00.05', 'a1','b1','s1'),

        (1, 1, '2023-02-15 00:00:00.06', 'a1','b1','s2'),
        (1, 3, '2023-02-15 00:00:00.07', 'a2','b2','s2'),
        (1, 3, '2023-02-15 00:00:00.08', 'a3','b3','s2'),
        (1, 1, '2023-02-15 00:00:00.09', 'a1','b1','s2'),
        (1, 4, '2023-02-15 00:00:00.10', 'a1','b1',null),

        (1, 1, '2023-02-15 00:00:00.11', 'a2','b2',''),
        (1, 4, '2023-02-15 00:00:00.12', 'a3','b3','s3'),
        (1, 1, '2023-02-15 00:00:00.13', 'a1','b1','s3'),
        (1, 2, '2023-02-15 00:00:00.14', 'a1','b1','s3'),
        (1, 5, '2023-02-15 00:00:00.15', 'a2','b2','s3'),

        (1, 1, '2023-02-15 00:00:00.16', 'a3','b3','s4'),
        (1, 5, '2023-02-15 00:00:00.17', 'a3','b3','s4'),
        (1, 2, '2023-02-15 00:00:00.18', 'a3','b3','s4'),
        (1, 4, '2023-02-15 00:00:00.19', 'a3','b3','s4'),
        (1, 3, '2023-02-15 00:00:00.20', 'a3','b3','s4'),

        (1, 1, '2023-02-15 00:00:00.21', 'a3','b3','s5'),
        (1, 1, '2023-02-15 00:00:00.22', 'a3','b3','s5'),
        (1, 4, '2023-02-15 00:00:00.23', 'a3','b3','s5'),
        (1, 5, '2023-02-15 00:00:00.24', 'a3','b3','s5'),
        (1, 3, '2023-02-15 00:00:00.25', 'a3','b3','s5'),

        (2, 3, '2023-02-15 00:00:00.26', 'a3','b3','s6'),
        (2, 2, '2023-02-15 00:00:00.27', 'a3','b3','s6'),
        (2, 5, '2023-02-15 00:00:00.28', 'a3','b3','s6')
        AS test(user_id,event_id,event_time,dim1,dim2,sid)
      ),
      tmp1 as (
        select user_id, user_path_analysis(
          event_time,
          case
          when event_id = 1  then 1
          when event_id = 2  then 2
          when event_id = 3  then 3
          when event_id = 4  then 4
          when event_id = 5  then 5
          else -1 end,
          case when event_id = 2  then dim1 else null end,
          'START',
          1,
          -- dim1 = 'a3' or dim2 = 'b2',
          true,
          sid,
          0
        ) seq
        from tmp0 group by user_id
      ),
      tmp2 as(
      select user_id,explode(seq) as up
      from tmp1
      ),
      tmp3 as(
      select
       user_id,(user_id || '-' || up.sid) as sid,up.seqNum,
       up.eid,up.eidBy,up.nextEid,up.nextEidBy
      from tmp2
      ),
      -- select * from tmp3 order by sid asc , seqNum asc
      select
         seqNum ,
         eid,
         eidBy,
         nextEid,
         nextEidBy,
        count(distinct sid) sid_cnt,
        compress_bitmap_build(user_id) bm
        from tmp3
        group by 1,2,3,4,5
        order by 1 asc,6 desc

    """.stripMargin
      )
      df.show(100, false)
    }
  }

}

object TypedImperativeAggregateSuite {

  /**
   * Calculate the max value with object aggregation buffer. This stores class MaxValue
   * in aggregation buffer.
   */
  private case class TypedMax(
                               child: Expression,
                               nullable: Boolean = false,
                               mutableAggBufferOffset: Int = 0,
                               inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[MaxValue] with ImplicitCastInputTypes {


    override def createAggregationBuffer(): MaxValue = {
      // Returns Int.MinValue if all inputs are null
      new MaxValue(Int.MinValue)
    }

    override def update(buffer: MaxValue, input: InternalRow): MaxValue = {
      child.eval(input) match {
        case inputValue: Int =>
          if (inputValue > buffer.value) {
            buffer.value = inputValue
            buffer.isValueSet = true
          }
        case null => // skip
      }
      buffer
    }

    override def merge(bufferMax: MaxValue, inputMax: MaxValue): MaxValue = {
      if (inputMax.value > bufferMax.value) {
        bufferMax.value = inputMax.value
        bufferMax.isValueSet = bufferMax.isValueSet || inputMax.isValueSet
      }
      bufferMax
    }

    override def eval(bufferMax: MaxValue): Any = {
      if (nullable && bufferMax.isValueSet == false) {
        null
      } else {
        bufferMax.value
      }
    }

    override lazy val deterministic: Boolean = true

    override def children: Seq[Expression] = Seq(child)

    override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType)

    override def dataType: DataType = IntegerType

    override def withNewMutableAggBufferOffset(newOffset: Int): TypedImperativeAggregate[MaxValue] =
      copy(mutableAggBufferOffset = newOffset)

    override def withNewInputAggBufferOffset(newOffset: Int): TypedImperativeAggregate[MaxValue] =
      copy(inputAggBufferOffset = newOffset)

    override def serialize(buffer: MaxValue): Array[Byte] = {
      val out = new ByteArrayOutputStream()
      val stream = new DataOutputStream(out)
      stream.writeBoolean(buffer.isValueSet)
      stream.writeInt(buffer.value)
      out.toByteArray
    }

    override def deserialize(storageFormat: Array[Byte]): MaxValue = {
      val in = new ByteArrayInputStream(storageFormat)
      val stream = new DataInputStream(in)
      val isValueSet = stream.readBoolean()
      val value = stream.readInt()
      new MaxValue(value, isValueSet)
    }
  }

  private class MaxValue(var value: Int, var isValueSet: Boolean = false)
}
