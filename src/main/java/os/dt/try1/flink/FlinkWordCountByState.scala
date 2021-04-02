package os.dt.try1.flink
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.table.api.Types
import org.apache.flink.util.Collector

/**
  *
  * Created by songgr on 2021/04/01.
  */
object FlinkWordCountByState {

  def main(args: Array[String]): Unit = {
    val data = Array(1,2,3,4,5)
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.fromElements(data:_*)
      .keyBy(v => v % 2)  // 分区 奇偶
      .process(new KeyedProcessFunction[Int, Int, Int] {
          // 定义state
          private var sumState:ValueState[Integer] = null
          override def open(parameters: Configuration): Unit = {
            super.open(parameters)
            val stateDesc = new ValueStateDescriptor[Integer]("sum", Types.INT)
            // 获取state
            sumState = getRuntimeContext.getState(stateDesc)
          }

          override def processElement(value: Int, ctx: KeyedProcessFunction[Int, Int, Int]#Context, out: Collector[Int]): Unit = {
            // 获取state值
            val oldSum = sumState.value()
            var sum = if (null == oldSum) Integer.valueOf(0) else oldSum
            sum += value
            // 更新state值
            sumState.update(sum)
            out.collect(sum)
          }
      }).print().setParallelism(2)

    environment.execute()
  }

}
