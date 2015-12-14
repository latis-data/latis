package latis.ops

import latis.dm._
import latis.metadata.Metadata

class Factorization extends Operation {

  //TODO: constructor arg?
  private var factor: String = null
  
  private def op(variable: Variable): Variable = variable match {
    case f: Function => {
      /*
       * TODO: require Index Function, for now
       * ds: i -> (t,a)
       * factor must be a range var (scalar)
       * 
       * ds.factorOut("t")
       *   t -> i -> (a)
       * .reduce
       *   t -> a (if length of every nested function is 1)
       * 
       * 
       */
      if (! f.getDomain.isInstanceOf[Index]) ???
      
      f.getRange match {
        case t: Tuple => {
          
        }
      }
      
      ???
    }

    case Sample(d,r) => {
      ???
    }
    
    /*
     * factor var out of tuple
     * TODO: handle interleaved Data
     * (a,b,c) factorOut("b") => (b,(a,c))
     * does returning a Sample make sense?
     */
    case Tuple(vars) => {
      val (dvars, rvars) = vars.partition(_.hasName(factor))
      if (dvars.length == 0) throw new Error("Factor not found in Tuple.")
      if (dvars.length > 1) throw new Error("Factor found more than once in Tuple.")
      val dvar = dvars.head
      
      rvars.length match {
        case 0 => variable //only had one element? no change, Sample with Index?
        case 1 => Tuple(dvar, rvars.head)
        case _ => Tuple(dvar, Tuple(rvars))
      }
    }
    
    case _ => ???
  }
  
  
}

object Factorization {
  
   def groupVariableBy(v: Variable, name: String): Function = v match {
//TODO: move to Operation
    //assumes Tuples don't contain data
    
//    case Sample(domain, range) => {
//      
//    }
    
    case Tuple(vars) => {
      val vs = vars.map(_.toSeq).flatten
      val (domain, range) = vs.partition(_.getName == name)
      domain.length match {
        case 1 => domain.head match {
          case s: Scalar => {
            val sample = Sample(s, Tuple(range))  //TODO: metadata
            Function(List(sample))
          }
          case _ => throw new Error("Can only group by Scalar variables, for now.")
        }
        case 0 => throw new Error("groupBy failed to find variable: " + name)
        case n: Int => throw new Error("groupBy found more than one Variable named: " + name)
      }
    }
    
    case f: Function => {
      //For each sample, make a new Function by factoring out the given variable to be the domain.
      //TODO: need Function? will this impl support a nested Function? 
      //recall that samples are Tuples 
      val new_samples = (for (sample <- f.iterator; new_sample <- groupVariableBy(sample,name).iterator) yield new_sample).toList
      //                     (I,(R,T))             (T,(I,R))
      //TODO: sort by domain, or let Function constructor do it?
      //TODO: need domain and range type, use PeekIterator with peek?  just suck in all samples as List
      
      //if duplicate values, need nested function
      //TODO: should this be done by Function constructor (along with sorting)? can't have duplicate domain samples
      //  but may just need index domain
      val map = new_samples.groupBy(_.domain.asInstanceOf[Scalar].getValue)
      //  Tval -> (T,(I,R))
      //TODO: do we dare use Scalar as key? need equals, hashCode
      
      //extract metadata from new domain
      val domainMetadata = new_samples.headOption match {
        case Some(s) => s.domain.getMetadata
        case None => Metadata.empty
      }
      
      val z = for ((value, samples) <- map) yield {
        //range should be the new_samples, need to remove the variable that is now the domain
        //  (myText, (myInteger, myReal))
        /*
         * start with I -> (R,T)
         * want T -> (I -> R)
         * groupBy(T)
         *   by sample.flatten: (I,R,T)
         *   T -> (I,R)
         *   
         * can range be a Sample and not just a Tuple? need a Sample match above?
         * samples: (T, (I,R))
         * 
         * can we know the I was a domain var and reconstruct the nested Function?
         * 
         */
        val domain = Scalar(domainMetadata, value)
        //TODO: preserve Time domain (LATIS-419)
        
        //val z = samples.map(_.range)
        //TODO: need these samples to be Samples, not just Tuples
        
        val ss = samples.map(sample => {
          // range: (T, (I,R))
          val vars = sample.range.asInstanceOf[Tuple].getVariables   // (I,R)
          Sample(vars.head, Tuple(vars.tail))  // I -> (R)  //TODO: reduce?
          //we know I was the orig domain, so make new function with these as the samples
        }) 
        val range = Function(ss)
        
        //Sample(Scalar(dvalue), Function(range.map(sample => Tuple(sample.getVariables.tail))))  //drop 1st var in tuple - now domain?
        Sample(domain, range)
      }
      //TODO: reduce function of one sample
      //simplify:
      
      
      //val z = map.map(p => Sample(Scalar(p._1), Function(p._2))).toList
      Function(z.toList)
    }
    
    //case _: Scalar => error?
  }
  
  //TODO: Dataset: def factorOut(factor: String): Dataset 
  def apply(factor: String) = {
    val f = new Factorization
    f.factor = factor
    f
  }
}