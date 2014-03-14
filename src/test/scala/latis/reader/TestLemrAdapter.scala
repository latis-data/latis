package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.Writer
import latis.ops._
import scala.collection.mutable.ArrayBuffer

class TestLmerAdapter {
  
  @Test
  def test_sparql {
    val ops = ArrayBuffer[Operation]()
    //ops += Selection("query=PREFIX+dcat:%3Chttp://www.w3.org/ns/dcat%23%3EPREFIX+dcterms:%3Chttp://purl.org/dc/terms/%3ESELECT?s?p?o+WHERE{?s+a+dcat:Dataset.?s+dcterms:identifier?name.+FILTER%28?name=%22sorce_tim_tsi_6hr_v15%22%29?s?p?o}")
    ops += Selection("query=PREFIX+dcat:<http://www.w3.org/ns/dcat%23>PREFIX+dcterms:<http://purl.org/dc/terms/>SELECT?s?p?o+WHERE{?s+a+dcat:Dataset.?s?p?o}")
    //ops += Selection("object=~sorce_tim_tsi_6hr_v15")
    ops += Selection("predicate=~.*version")
    ops += Selection("object=~11")
    val ds = TsmlReader("datasets/test/sparql.tsml").getDataset(ops)
    
    Writer("asc").write(ds)
  }
}