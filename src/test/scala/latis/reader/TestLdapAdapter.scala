package latis.reader

import org.junit._
import Assert._
import latis.reader.tsml.TsmlReader
import latis.writer.Writer
import latis.ops._
import scala.collection.mutable.ArrayBuffer

class TestLdapAdapter {
  
  //@Test
  def test_ldap {
    //TODO: index name "unknown"
    //TODO: use "sequence"
    val ops = ArrayBuffer[Operation]()
    ops += Selection("memberOf=~.*maven-sdc-web-users.*")
    ops += Projection("uid,cn,mail")
    val ds = TsmlReader("datasets/test/ldap_users.tsml").getDataset(ops)
    
    Writer.fromSuffix("csv").write(ds)
  }
}