package org.stsffap.benchmarks

case class RuntimeConfiguration(outputPath: String, checkpointDir: Option[String] = None,
                                iterationsUntilCheckpoint: Int = 0) {

}
