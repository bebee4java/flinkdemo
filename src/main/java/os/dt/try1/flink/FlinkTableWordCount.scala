package os.dt.try1.flink

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

object FlinkTableWordCount {


  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)


    val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
    val expr = input.toTable(tEnv)
    val result = expr
      .groupBy('word)
      .select('word, 'frequency.sum as 'frequency)
      .filter('frequency === 2)
      .toDataSet[WC]

    result.print()
  }

  case class WC(word: String, frequency: Long)
}



