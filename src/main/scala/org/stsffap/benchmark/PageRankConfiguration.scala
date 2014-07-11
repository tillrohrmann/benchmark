package org.stsffap.benchmark

case class PageRankConfiguration(val numRows: Int =0, val sparsity: Double=0.0, val maxIterations: Int=0,
                                 val outputPath: String = "") {

}
