package latis.ops

import latis.data.set.DomainSet
import latis.dm.Function
import latis.dm.Sample
import latis.dm.Variable

class Resample(domain: Variable, set: DomainSet) extends Operation {
  //TODO: more nouny?
  //TODO: strategies: equals, nearest, linear, ...  maybe its own package?
  //TODO: sanity check that domain set is ordered? enforce in DomainSet constructor
  
  /*
   * TODO: apply to samples vs getting domain set of Dataset
   * need 2 samples for strategies other than equality
   * delegate to overridable function for diff strategies
   * 
   */
  
  override def applyToFunction(function: Function): Option[Variable] = {
    //apply if this Function has the domain type we are interested in
    if (function.getDomain.hasName(domain.getName)) {
      //Make new function with new samples, one for each element of the domain set
      //assume a 1D numeric domain for now
      //TODO: but can't assume data represents a double, match on Number(d)? but 'domain' doesn't have the data
      
      //iterate over each datum in the new domain set
      val it = set.iterator.map(data => {
        
      })
      
      
    } else {
      //TODO: apply to range (super?) to catch nested Functions
    }
    
    ???
  }
  

}