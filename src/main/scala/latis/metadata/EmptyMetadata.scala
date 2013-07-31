package latis.metadata

object EmptyMetadata extends Metadata {
  def get(key: String) = None
}