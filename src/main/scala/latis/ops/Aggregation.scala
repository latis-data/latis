package latis.ops

import latis.dm.Dataset

/*
 * TODO: 2013-10-07
 * multiple dimensions of aggregation
 * from relational algebra perspective: attributes (columns/variables)
 *   union to combine all vars, intersect to keep common vars
 *   join?
 * from time series perspective: time dimension
 *   union: append, interleave, overwrite?, assume same vars
 *   intersect: common time samples, diff vars
 *   union with diff vars: end up needing to fill
 * higher dims, e.g. grids
 *   union: append = tile
 * time series of spectra (as opposed to 2D grid)
 *   new time samples: must new spectra have same samples?
 *   new wl samples: same times
 *   still largely a tiling issue
 *   
 * conglomeration: keep dataset structures as tuples
 *   new dataset is a tuple with a element for each 
 * 
 * other verbs:
 *   concatenate
 *   merge
 *   join: all combinations (pairs)?
 *     natural join: join on matching common column values
 *   combine
 */

trait Aggregation extends Operation {
  def aggregate(dataset: Dataset): Dataset
  
  def apply(dataset: Dataset) = aggregate(dataset)
  def apply(datasets: Seq[Dataset]) = aggregate(datasets)
  def apply(dataset1: Dataset, dataset2: Dataset) = aggregate(dataset1, dataset2)
  
  def aggregate(datasets: Seq[Dataset]): Dataset = aggregate(Dataset(datasets))
  def aggregate(dataset1: Dataset, dataset2: Dataset): Dataset = aggregate(List(dataset1, dataset2))
}
