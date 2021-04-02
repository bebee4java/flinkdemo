package os.dt.try1.flink

import java.util

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
  *
  * Created by songgr on 2021/04/01.
  */
object FlinkWindowDemo {

  def main(args: Array[String]): Unit = {
    val data = Array[Int](1,2,3,4,5)
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val source = environment.addSource(new SourceFunction[Int] {
      private var stop = false
      // 自己构造流
      override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
        var i:Int = 0
        while (!stop && i < data.length) {
          ctx.collect(data(i))
          i += 1
          // 增加数据的一些延时
          Thread.sleep(200)
        }
      }
      override def cancel(): Unit = {stop = true}
    })

    // 增加 processing 时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    source
      .keyBy(v => v % 2)
      .process(new KeyedProcessFunction[Int, Int, Int] {
        // 自定义Window
        private val WINDOW_SIZE = 200
        private var window:java.util.TreeMap[Long, Int] = null

        override def open(parameters: Configuration): Unit = {
          super.open(parameters)
          window = new util.TreeMap[Long, Int]()
        }

        override def processElement(value: Int, ctx: KeyedProcessFunction[Int, Int, Int]#Context, out: Collector[Int]): Unit = {
          val currentTime = ctx.timerService().currentProcessingTime()
          val windowKey = currentTime / WINDOW_SIZE
          // update window value
          val sum = window.getOrDefault(windowKey, 0)
          window.put(windowKey, sum + value)

          // print old window
          val oldWindow = window.headMap(windowKey, false)
          val iterator = oldWindow.entrySet().iterator()
          while (iterator.hasNext) {
            // out
            out.collect(iterator.next().getValue)
            // remove old value
            iterator.remove()
          }

        }

        override def close(): Unit = {
          super.close()
          // 输出剩余window值
          println(window)
        }
      }).print().setParallelism(2)

    environment.execute()
  }

}
