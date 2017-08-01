package latis.reader

import org.junit._
import Assert._
import java.sql._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import scala.collection.mutable.ArrayBuffer
import latis.ops._
import latis.ops.filter._
import latis.dm._
import latis.writer.Writer
import javax.naming.NameNotFoundException
import javax.naming.NoInitialContextException
import latis.reader.tsml.TsmlReader2
import java.net.URL

class TestJdbcAdapter extends AdapterTests {
  def datasetName = "db"
    
  @Test
  def request_by_time {
    val ops = List(Selection("time>1970-01-02T00:00:00"))
    val data = getDataset(ops).toStringMap
    assertEquals("1970-01-03 00:00:00.000", data("myTime").head)
  }
  
  //@Test
  //TODO: Selection constructor removes white space
  def request_by_native_time {
    val ops = List(Selection("myTime>'1970-01-02 00:00:00'"))
    val data = getDataset(ops).toStringMap
    assertEquals("1970-01-03 00:00:00.000", data("myTime").head)
  }
  
  //TODO: move rename tests to AdapterTests once it supports general variables
  
   @Test
  def rename_range_variable {
    val ops = List(RenameOperation("myText", "theText"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("theText").length)
  }  
  
  @Test
  def rename_time_variable {
    val ops = List(RenameOperation("myTime", "theTime"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data("theTime").length)
  }
  
  @Test
  def project_then_rename {
    val ops = List(Projection("myTime,myInt"), RenameOperation("myInt", "theInt"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data.keySet.size)
    assertEquals(3, data("theInt").length)
  }
  
  @Test
  def rename_then_project {
    //projection likely needs to use the orig names for jdbc adapter? //TODO: make consistent with general op application
    val ops = List(RenameOperation("myTime", "theTime"), Projection("myTime,myText"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data.keySet.size)
    assertEquals(3, data("theTime").length)
  }
  
  @Test
  def derive_then_project {
    val ops = List(MathExpressionDerivation("i2=myInt^2"), Projection("myTime,myInt,i2"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data.keySet.size)
    assertEquals(3, data("myTime").length)
    assertEquals(9.0, data("i2")(2).toDouble, 0.0)
  }
  
  @Test
  def project_then_derive {
    val ops = List(Projection("myTime,myInt,i2"),MathExpressionDerivation("i2=myInt^2"))
    val data = getDataset(ops).toStringMap
    assertEquals(3, data.keySet.size)
    assertEquals(3, data("i2").length)
    assertEquals(4.0, data("i2")(1).toDouble, 0.0)
  }
  
  @Test
  def select_with_orig_name_then_rename {
    val ops = List(Selection("myInt>1"), RenameOperation("myInt", "theInt"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("theInt").length)
  }
  
  @Test
  def rename_then_select {
    //Note, Selection must use orig name for now
    val ops = List(RenameOperation("myInt", "theInt"), Selection("myInt>1"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("theInt").length)
  }
  
  @Test
  def select_by_native_time {
    //need a time var defined as an int
    val ops = List(Selection("myInt>1"))
    val ds = TsmlReader("db_with_int_time.tsml").getDataset(ops)
    val data = ds.toStringMap
    assertEquals(2, data("myInt").length)
  }
  
  @Test
  def predicate_in_tsml {
    val ops = List(Selection("myInt>1"))
    val ds = TsmlReader("db_with_predicate.tsml").getDataset(ops)
    val data = ds.toStringMap
    assertEquals(1, data("myInt").length)
  }
  
  @Test //TODO: deprecate use of quotes?
  def select_by_text_value_with_single_quotes {
    val ops = List(Selection("myText='B'"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("myInt").head.toInt)
  }
  
  //@Test //double quotes not supported so don't encourage anyone
  def select_by_text_value_with_double_quotes {
    val ops = List(Selection("""myText="B""""))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("myInt").head.toInt)
  }
  
  @Test
  def select_by_text_value_without_quotes {
    val ops = List(Selection("myText=B"))
    val data = getDataset(ops).toStringMap
    assertEquals(2, data("myInt").head.toInt)
  }
  
  @Test
  def location_java_prefixed {
    // Finding a "java:" prefix in the location="..." attribute means we should
    // load the connection via jndi. We don't expect jndi to work outside of
    // a JavaEE container (web server) so the best we can do is check that
    // the correct exceptions are thrown.
    var wasCaught = false
    val ops = List(Selection("foo>1"))
    try {
      val ds = TsmlReader("db_with_bad_java_location.tsml").getDataset(ops)
    }
    catch {
      case e1: NoInitialContextException => {
        // this means we attempted to find something via JNDI but failed because there
        // was no initial context (prob b/c we're in unit tests). This means we hit
        // the right branch so, even though this is ugly, pass!
        wasCaught = true
        assert(true) // pass
      }
      case e2: Error => {
        // this is the error re-thrown by JdbcAdapter.getConnectionViaJndi when it gets a
        // NameNotFoundException. This means we hit the right branch, so pass!
        wasCaught = true
        assertEquals("JdbcAdapter failed to locate JNDI resource: bad_java_location", e2.getMessage)
      }
    }
    assertTrue("An Error should have been thrown", wasCaught)
  }
  
  @Test
  def location_is_nonsensical {
    var wasCaught = false
    val ops = List(Selection("foo>1"))
    try {
      val ds = TsmlReader("db_with_completely_wonky_location.tsml").getDataset(ops)
    }
    catch {
      case e1: RuntimeException => {
        wasCaught = true
        assertEquals(
          "Unable to find or parse tsml location attribute: location must exist and start with 'java:' (for JNDI) or 'jdbc:' (for JDBC)",
          e1.getMessage
        )
      }
    }
    assertTrue("An Error should have been thrown", wasCaught)
  }
  
  @Test
  def jndi_attr {
    // Finding a jndi="..." attribute means we should
    // load the connection via jndi. We don't expect jndi to work outside of
    // a JavaEE container (web server) so the best we can do is check that
    // the correct exceptions are thrown.
    var wasCaught = false
    val ops = List(Selection("foo>1"))
    try {
      val ds = TsmlReader("db_with_jndi_attr.tsml").getDataset(ops)
    }
    catch {
      case e: RuntimeException => {
        //We no longer support jndi 
        wasCaught = true
        assertEquals(
          "Unable to find or parse tsml location attribute: location must exist and start with 'java:' (for JNDI) or 'jdbc:' (for JDBC)"
          , e.getMessage)
      }
    }
    assertTrue("An Error should have been thrown", wasCaught)
  }

  @Test
  def test_limit_filter_negative {
    var wasCaught = false
    val ops = List(LimitFilter(-1))
    try {
      val ds = getDataset(ops)
    }
    catch {
      case e: UnsupportedOperationException => wasCaught = true
    }
    assertTrue("An Error should have been thrown", wasCaught)
  }

  @Test
  def test_limit_filter_zero {
    val ops = List(LimitFilter(0))
    val ds = getDataset(ops)
    val expected = 0
    ds match {
      case Dataset(Function(it)) => assertEquals(expected, it.length)
    }
  }

  @Test
  def test_limit_filter {
    val ops = List(LimitFilter(2))
    val ds = getDataset(ops)
    val expected = 2
    ds match {
      case Dataset(Function(it)) => assertEquals(expected, it.length)
    }
  }

  @Test
  def test_limit_filter_greater_than_dataset {
    val ops = List(LimitFilter(100))
    val ds = getDataset(ops)
    val expected = 3
    ds match {
      case Dataset(Function(it)) => assertEquals(expected, it.length)
    }
  }


  @Test
  def test_user_doesnt_override_limit {
    val ops = List(LimitFilter(3))
    val ds = TsmlReader("datasets/test/db2.tsml").getDataset(ops)
    val expected = 2
    ds match {
      case Dataset(Function(it)) => assertEquals(expected, it.length)
    }
  }
  
  //@Test
  def iterative3 = {
    val ops = scala.collection.mutable.ArrayBuffer[Operation]()
  //TODO: still broken  ops += Selection("myTime >= 1970-01-02")
    ops += Projection("myInt,myTime")
    ops += Selection("MYREAL > 2")
    //ops += Selection("myInt > 2")
    //ops += TimeFormatter("yyyy-MM-dd")
    val ds = TsmlReader2.fromName("db3").getDataset(ops)
    latis.writer.Writer.fromSuffix("asc").write(ds)
  }
}

object TestJdbcAdapter {
  
  //private var connection: Connection = null
  
  @BeforeClass
  def makeDatabase {
    //TODO: make sure these data are consistent with data used in other tests, ingest from same ascii file?
    //TODO: add Text type with length longer than default (4)

    System.setProperty("derby.stream.error.file", "/dev/null") //don't make log file
    
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver")  //Load the JDBC driver     
    val connection = DriverManager.getConnection("jdbc:derby:memory:testDB;create=true")
    
    var statement = connection.createStatement()
    statement.execute("""create table test(myTime timestamp, myInt int, myReal double, myText varchar(1))""")
    statement.execute("insert into test values('1970-01-01 00:00:00', 1, 1.1, 'A')")
    statement.execute("insert into test values('1970-01-02 00:00:00', 2, 2.2, 'B')")
    statement.execute("insert into test values('1970-01-03 00:00:00', 3, 3.3, 'C')")
    
//    statement = connection.createStatement()
//    val rs = statement.executeQuery("select * from test where time > '1970-01-01T00:00:00'") 
//    //TODO: derby only works with '1970-01-01 00:00:00' !?  based on java.sql.Timestamp
//    while (rs.next()) {
//      println(rs.getString(1) + " " + rs.getInt(2) + " " + rs.getDouble(3) + " " + rs.getString(4))
//    }
  }
  
  @AfterClass
  def dropDatabase {
    try {
      DriverManager.getConnection("jdbc:derby:memory:testDB;drop=true")
    } catch {
      case e: Exception =>
    }
  }
}
