package latis.metadata

class OperationDescription(
    val name: String, 
    val className: String, 
    val description: String, 
    val usage: String) { }

object OperationDescription {
  def apply(
      name: String, 
      className: String, 
      description: String, 
      usage: String): OperationDescription = 
        new OperationDescription(name, className, description, usage)
}
