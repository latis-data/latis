package latis.reader.adapter

case class AdapterConfig(properties: Map[String,String], processingInstructions: Seq[ProcessingInstruction]) {
  
}

case class ProcessingInstruction(name: String, args: String)

