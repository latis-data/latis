package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.Writer
import latis.ops._
import latis.ops.filter._
import scala.collection.mutable.ArrayBuffer
import latis.writer.AsciiWriter

class TestLmerAdapter {
  
  @Test
  def test_sparql_in_url {
    val ops = ArrayBuffer[Operation]()
    //ops += Selection("query=PREFIX+dcat:%3Chttp://www.w3.org/ns/dcat%23%3EPREFIX+dcterms:%3Chttp://purl.org/dc/terms/%3ESELECT?s?p?o+WHERE{?s+a+dcat:Dataset.?s+dcterms:identifier?name.+FILTER%28?name=%22sorce_tim_tsi_6hr_v15%22%29?s?p?o}")
    ops += Selection("query=PREFIX dcat:<http://www.w3.org/ns/dcat#> PREFIX dcterms:<http://purl.org/dc/terms/> SELECT ?s?p?o WHERE{?s a dcat:Dataset.?s?p?o}")
    //ops += Selection("query=PREFIX dcathttp//www.w3.org/ns/dcat")
    //ops += Selection("object=~sorce_tim_tsi_6hr_v15")
    ops += Selection("predicate=~.*version")
    ops += Selection("object=~11")
    val ds = TsmlReader("datasets/test/lemr.tsml").getDataset(ops)

    assertEquals(10, ds.length)
    
    //TODO: FilteredFunction needs to redo index
    //Writer.fromSuffix("asc").write(ds)
  }
  
  //@Test
  def test_sparql_in_tsml {
    val ops = ArrayBuffer[Operation]()
    val ds = TsmlReader("datasets/test/lemr_datasets.tsml").getDataset(ops)
    
    val ds2 = ds.groupBy("subject")
    
    AsciiWriter.write(ds2)
  }
}