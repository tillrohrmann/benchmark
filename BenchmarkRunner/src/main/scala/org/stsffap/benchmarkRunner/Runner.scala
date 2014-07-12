package org.stsffap.benchmarkRunner

import java.io.{File, PrintStream}

import eu.stratosphere.api.common.PlanExecutor
import eu.stratosphere.client.RemoteExecutor
import org.apache.spark.SparkConf
import org.ini4j.Ini
import org.stsffap.benchmarks.pageRank.{PageRankSpark, PageRankStratosphere}
import org.stsffap.benchmarks.{Benchmark, RuntimeConfiguration, Benchmarks}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object Runner {
  val DEFAULT_BENCHMARK = "PageRank"
  val DEFAULT_ENGINE = "Stratosphere"
  val DEFAULT_MASTER = "node1"
  val DEFAULT_APPNAME = "Benchmark"
  val DEFAULT_OUTPUT_PATH = "file:///tmp/benchmark"
  val DEFAULT_OUTPUT_FILE = "benchmarkResult"
  val DEFAULT_SPARK_PORT = 7077
  val DEFAULT_STRATOSPHERE_PORT = 6123

  var benchmark: Benchmarks.Value = null
  var engine:Engines.Value = null
  var master:String = DEFAULT_MASTER
  var appName: String = DEFAULT_APPNAME
  var outputPath: String = DEFAULT_OUTPUT_PATH
  var outputFile: String = DEFAULT_OUTPUT_FILE
  var parallelism: List[Int] = null
  var tries: Int = 0;
  var port:Option[Int] = None
  val data = collection.mutable.HashMap[String, List[String]]()
  var memory:Option[String] = None
  var libraryPath: String = null

  var datapoints: ListBuffer[DatapointEntry] = ListBuffer()



  def main(args: Array[String]){
    if(args.length <1){
      printUsage()
    }else{
      val file = new File(args(0))
      val ini = new Ini(file)

      processGeneralConfig(ini)
      processData(ini)
      runBenchmark()
      printResults()
    }
  }

  def runBenchmark(){
    val length = math.max(getDataLength, parallelism.length)

    for(idx <- 0 until length){
      val inputData = getData(idx)
      val p = getParallelism(idx)

      val benchmark = instantiateBenchmark(p)
      val runtimeConfig = RuntimeConfiguration(outputPath)

      val measurements = for(_ <- 0 until tries) yield {
        benchmark.run(runtimeConfig, inputData)
      }

      benchmark.stop()

      val cleanedMeasurements = measurements.filter{_ >= 0}
      val num = cleanedMeasurements.length
      val average = cleanedMeasurements.fold(0.0)(_+_)/num
      val stdEstimate = if(num > 1){
        math.sqrt(1.0/(num-1)* cleanedMeasurements.
          foldLeft(0.0){ (s, e) => s + math.pow((e - average),2)})
      } else {
        0
      }

      datapoints += DatapointEntry(inputData.+(("time", average.toString)).+(("error",
        stdEstimate.toString)).+(("parallelism", p.toString)))
    }
  }

  def instantiateBenchmark(parallelism: Int): Benchmark = {
    engine match {
      case Engines.Stratosphere =>
        val executor: PlanExecutor = new RemoteExecutor(master, port match {
          case Some(p) => p
          case None => DEFAULT_STRATOSPHERE_PORT
        }, getStratosphereDependencies)

        benchmark match {
          case Benchmarks.PageRank => new PageRankStratosphere(executor, parallelism)
          case Benchmarks.NMF => null
          case Benchmarks.KMeans => null
        }

      case Engines.Spark =>
        val masterURL = "spark://" + master + ":" + (port match {
          case Some(p) => p
          case _ => DEFAULT_SPARK_PORT
        })

        val conf = new SparkConf().
        setAppName(appName).
        setMaster(masterURL).
        set("spark.cores.max", parallelism.toString).
        set("spark.default.parallelism", parallelism.toString).
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        setJars(getSparkDependencies)

        memory foreach { m => conf.set("spark.executor.memory", m)}

        benchmark match {
          case Benchmarks.PageRank => new PageRankSpark(conf)
          case Benchmarks.KMeans => null
          case Benchmarks.NMF => null
        }
    }
  }

  def getSparkDependencies: List[String] = {
    List("benchmarks-1.0-SNAPSHOT.jar") map { x => libraryPath + x }
  }

  def getStratosphereDependencies: List[String] = {
    List("benchmarks-1.0-SNAPSHOT.jar", "breeze_2.10-0.8.1.jar", "commons-math3-3.2.jar") map { x => libraryPath + x}
  }

  def printResults() {
    val file = new File(outputFile)

    val printStream = new PrintStream(file)

    printStream.println(instantiateBenchmark(0).getInformation)

    val firstDatapoint = datapoints(0)

    val header = firstDatapoint.usedData.keys.toList

    printStream.println(header.mkString(" "))

    for(datapoint <- datapoints) {
      printStream.println(header map { entry => datapoint.usedData(entry) } mkString(" "))
    }

    printStream.close()
  }

  def getDataLength: Int = {
    var max = 0;

    for(value <- data.values){
      if(max < value.length){
        max = value.length
      }
    }

    max
  }

  def getData(idx: Int): Map[String, String] = {
    val l = for((key, values) <- data.iterator)yield{
      if(values.length <= idx){
        (key, values.last)
      }else{
        (key, values(idx))
      }
    }

    l.toMap
  }

  def getParallelism(idx: Int): Int = {
    if(idx < parallelism.length){
      parallelism(idx)
    }else{
      parallelism.last
    }
  }

  def processData(ini: Ini){
    val dataSection = ini.get("data")

    val entries = dataSection.keySet()

    for(entry <- entries){
      val value = dataSection.get(entry).split(",").map{ value => value.trim }
      data.put(entry, value.toList)
    }
  }


  def processGeneralConfig(ini: Ini){
    val generalSection = ini.get("general")
    benchmark = generalSection.get("benchmark", DEFAULT_BENCHMARK) match {
      case "PageRank" => Benchmarks.PageRank
      case "NMF" => Benchmarks.NMF
      case "KMeans" => Benchmarks.KMeans
    }
    engine = generalSection.get("engine", DEFAULT_ENGINE) match {
      case "Stratosphere" => Engines.Stratosphere
      case "Spark" => Engines.Spark
    }
    master = generalSection.get("master", DEFAULT_MASTER)
    appName = generalSection.get("appName", DEFAULT_APPNAME)
    outputPath = generalSection.get("outputPath", DEFAULT_OUTPUT_PATH)
    outputFile = generalSection.get("outputFile", DEFAULT_OUTPUT_FILE)
    parallelism = generalSection.get("parallelism", "1").split(",").map(x => x.trim().toInt).toList
    tries = generalSection.get("tries", "1").toInt
    port = generalSection.get("port") match {
      case null => None
      case x => Some(x.toInt)
    }

    memory = generalSection.get("memory") match {
      case null => None
      case x => Some(x)
    }

    this.libraryPath = generalSection.get("libraryPath", new File(this.getClass().getProtectionDomain().getCodeSource
      ().getLocation().getFile()).getParent()+"/lib/")
  }

  def printUsage() {
    println("Runner <settings File>")
  }
}
