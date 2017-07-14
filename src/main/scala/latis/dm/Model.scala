package latis.dm

import latis.metadata.Metadata

/*
 * New representation of information in a tsml file.
 * Created after a first pass in an adapter.
 */


trait VariableType
//TODO: need to expose id, metadata? or just rely on pattern matches

case class ScalarType(
    id: String, 
    metadata: Metadata = Metadata.empty
    ) extends VariableType
//TODO: put type in metadata? useful for readers of json...
//  type could be a known type or a class name that extends Scalar
//TODO: allow data to be defined?
//TODO: make sure metadata has name, default to id
//  but can't do here with immutable metadata, use smart constructor?

case class TupleType(
    vars: Seq[VariableType],
    id: String = "",
    metadata: Metadata = Metadata.empty
    ) extends VariableType
//TODO: allow anonymous tuple? no name in metadata, 
//    but is id still required? "" or auto?


case class FunctionType(
    domain: VariableType,
    codomain: VariableType,
    id: String = "", 
    metadata: Metadata = Metadata.empty
    ) extends VariableType


case class ProcessingInstruction(
    name: String,
    args: String,
    targetVariable: String = "" //apply to dataset if ""
    )

case class Model(
    v: VariableType,  //top level variable
    metadata: Metadata = Metadata.empty,
    pis: Seq[ProcessingInstruction] = Seq.empty
    ) {
  
  
}
//TODO: dataset joins
//TODO: construct from model syntax: X -> (A, B)
    //TODO: name registry, guarantee unique names? 
    //  generate scalar1, scalar2...? uuid?

