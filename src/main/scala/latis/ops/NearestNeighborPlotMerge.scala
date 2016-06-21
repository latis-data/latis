package latis.ops

import latis.dm._

import latis.ops.resample.NearestNeighborResampling2D

class NearestNeighborPlotMerge extends PlotMerge {
  
  override def applyResampling(domain: Iterable[Variable]): Operation = {
    new NearestNeighborResampling2D(domain)
  }
}

