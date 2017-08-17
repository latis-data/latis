package latis.reader.adapter

import latis.dm._
import latis.ops._

abstract class Adapter3(model: Dataset3, config: AdapterConfig) {
  
  /**
   * Abstract method to remind subclasses that they need 
   * to clean up their resources.
   */
  def close: Unit
  
  /**
   * Offer an Adapter the chance to handle a processing instruction.
   * It should return true to prevent the default application.
   */
  def handlePI(pi: ProcessingInstruction): Boolean = false
    
  /**
   * Hook for subclasses to apply operations during data access
   * to reduce data volumes. (e.g. query constraints to database or web service)
   * Return true if it will be handled. Otherwise, it will be applied
   * to the Dataset by the Adapter class.
   * The default behavior is for the Adapter subclass to handle no operations.
   */
  def handleOperation(op: Operation): Boolean = false
  
  /**
   * Main entry point for the Reader to request the Dataset
   * with the given sequence of operation applied.
   */
  def getDataset(operations: Seq[Operation]): Dataset3 = {
    // Allow subclasses to handle ProcessingInstructions.
    val unhandledPIs: Seq[ProcessingInstruction] = config.processingInstructions.filterNot(handlePI(_))
    
    // Allow subclasses to handle user Operations.
    val unhandledOps: Seq[Operation] = operations.filterNot(handleOperation(_))
      
    // Hook for subclasses to do some things before making the Dataset.
    //preMakeDataset
    
    // Construct the Dataset based on the Model.
    val dataset = makeDataset(model)

//    // Apply remaining processing instructions and user Operations.
//    val piOps = unhandledPIs.map(pi => Operation(pi.name, pi.args.split(',').map(_.trim)))
//    //TODO: deal with PIs targeted for specific var 
//    (piOps ++ unhandledOps).foldLeft(dataset)((ds, op) => op(ds))
//    //TODO: compose (and optimize) Operations (as functions V => V) then apply to dataset
    dataset
  }
  
  //---- Construct Dataset ----------------------------------------------------

  def makeDataset(model: Dataset3): Dataset3 = {
    ???
  }
  
}

