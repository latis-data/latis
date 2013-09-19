package latis.reader.tsml

import scala.xml.Elem
import latis.dm._
import scala.collection.mutable.ArrayBuffer

class UnionAggregator(tsml: Tsml) extends Aggregator(tsml) {

  override def aggregate(datasets: Seq[Dataset]): Dataset = {
    /*
     * assume each dataset contains a single Function: Real -> Real with consecutive domain samples
     * return dataset with the longer Function: Real -> Real
     */
    
    
//    //concatenate domain samples
//    val domains: Seq[DomainSet] = datasets.map(_(0).asInstanceOf[Function].domain)
//    val dvars = domains.fold(new ArrayBuffer[Variable]())((a,b) => a ++ b)
//    val domain = DomainSet(dvars)
//    
//    //concatenate range samples
//    val ranges = datasets.map(_(0).asInstanceOf[Function].range) //Seq[VariableSeq]
//    val rvars = ranges.fold(new ArrayBuffer[Variable]())((a,b) => a ++ b)
//    val range = VariableSeq(rvars)
//    
//    val f = Function(domain, range)
    
    //metadata, largely the same for each dataset, just use first
//    val md = datasets(0).getVariableByIndex(0).metadata.asInstanceOf[FunctionMd]
//    
//    //TODO: just concatenate function iterators?
//    val fs: Seq[Iterator[(Variable,Variable)]] = datasets.map(_(0).asInstanceOf[Function].iterator)
//    val fit = fs.fold(Iterator.empty)((a,b) => a ++ b)
//    val f = Function(md, fit)
//    
//    Dataset(f)
    ???
  }
  
}