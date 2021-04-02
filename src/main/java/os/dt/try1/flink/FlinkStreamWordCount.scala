package os.dt.try1.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
  *
  * Created by songgr on 2021/04/01.
  */
object FlinkStreamWordCount {

  def main(args: Array[String]): Unit = {
    val data = Array("hello word", "hello flink", "flink is ok")
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.fromElements(data:_*).flatMap(_.split(" "))
      .map(it => (it, 1)).keyBy(_._1).sum(1)
      .print()

    environment.execute()
  }

}
