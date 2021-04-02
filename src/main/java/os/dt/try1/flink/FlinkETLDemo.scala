package os.dt.try1.flink

import org.apache.flink.streaming.api.functions.source.FromElementsFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Types
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction

/**
  *
  * Created by songgr on 2021/04/01.
  */
object FlinkETLDemo {

  def main(args: Array[String]): Unit = {
    // 构造数据
    val data = Array[Integer](1,2,3,4,5)
    // 获取执行坏境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    // add 数据源
    val source = environment.addSource(new FromElementsFunction[Integer](Types.INT.createSerializer(environment.getConfig), data:_*))
    val ds = source.map(i => i*2).keyBy(v => 1).sum(0)
    // add print sink
    ds.addSink(new PrintSinkFunction[Int]())

    println(environment.getExecutionPlan)
    // 执行
    environment.execute()
  }

}
