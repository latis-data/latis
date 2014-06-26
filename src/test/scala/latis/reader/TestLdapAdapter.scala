package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.Writer
import latis.ops._
import latis.ops.filter._
import scala.collection.mutable.ArrayBuffer
import latis.writer.AsciiWriter

class TestLdapAdapter {
  
  @Test
  def test_ldap {
    //TODO: index name "unknown"
    //TODO: use "sequence"
    val ops = ArrayBuffer[Operation]()
    //ops += Selection("memberOf=~.*maven-sdc-web-users.*")
    ops += Projection("uid,cn,mail")
    val ds = TsmlReader("datasets/test/ldap.tsml").getDataset(ops)
    
    val data = ds.toStringMap
    val i = data("uid").indexOf("lindholm")
    
    assertEquals("Doug.Lindholm@lasp.colorado.edu", data("mail")(i))
    
    //AsciiWriter.write(ds)
  }
}