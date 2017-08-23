package latis.reader

import latis.reader.tsml._
import org.junit._
import Assert._
import latis.writer.AsciiWriter
import java.net.URL
import latis.ops._
import latis.dm._
import latis.metadata.Metadata

class TestLisirdLEMRCatalogGenerator {
  
  @Test
  def check_lemr_catalog = {
    val ds = LisirdLemrCatalogGenerator().getDataset 

    //AsciiWriter.write(ds)
    ds match {
      case Dataset(Function(it)) if it.hasNext => it.next match {
        case Sample(Text(name), TupleMatch(Text(description), TupleMatch(Text(accessURL)))) => {
          assertEquals("cak", name)
          assertEquals("", description)
          assertEquals("cak", accessURL)
        }
        case _ => fail
      }
      case _ => fail
    }
  }
  
  
}
