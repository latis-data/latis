package latis.reader

import org.junit._
import Assert._
import java.sql._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import scala.collection.mutable.ArrayBuffer
import latis.ops._
import latis.dm._
import latis.writer.Writer

class TestJdbcAdapter {

  private var connection: Connection = null
  
  @BeforeClass
  def makeDatabase {
    //TODO: make sure these data are consistent with data used in other tests, ingest from same ascii file?
    //TODO: add Text type with length longer than default (4)
    
    System.setProperty("derby.stream.error.file", "/dev/null") //don't make log file
    
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver")  //Load the JDBC driver     
    connection = DriverManager.getConnection("jdbc:derby:memory:testDB;create=true")
    
    var statement = connection.createStatement()
    statement.execute("create table test(t timestamp, i int, d double, s varchar(1))")
    statement.execute("insert into test values('1970-01-01 00:00:00', 1, 1.1, 'A')")
    statement.execute("insert into test values('1970-01-02 00:00:00', 2, 2.2, 'B')")
    statement.execute("insert into test values('1970-01-03 00:00:00', 3, 3.3, 'C')")
  }
  
  @AfterClass
  def dropDatabase {
    try {
      DriverManager.getConnection("jdbc:derby:memory:testDB;drop=true")
    } catch {
      case e: Exception =>
    }
  }
  
  //@Test
  def read_db {
    var statement = connection.createStatement()
    val rs = statement.executeQuery("select * from test")
    
    while (rs.next()) {
      println(rs.getInt(1) + " " + rs.getDouble(2) + " " + rs.getString(3) + " " + rs.getString(4))
    }
  }
  
  
  //@Test
  def select_data_range {
    val ops = List(Selection("d>2"))
    val ds = TsmlReader("datasets/test/db.tsml").getDataset(ops)
    AsciiWriter.write(ds)
  }
  
  //@Test
  def project {
    val ops = List(Projection("t,s"))
    val ds = TsmlReader("datasets/test/db.tsml").getDataset(ops)
    AsciiWriter.write(ds)
  }
  
  //@Test
  def dont_project_domain {
    val ops = List(Projection("d,s"))
    val ds = TsmlReader("datasets/test/db.tsml").getDataset(ops)
    AsciiWriter.write(ds)
  }
  
  //@Test
  def string_match {
    val ops = List(Selection("s=~A"))
    val ds = TsmlReader("datasets/test/db.tsml").getDataset(ops)
    AsciiWriter.write(ds)
  }
  
  //@Test
  def iso_time_selection {
    val ops = List(Selection("time>=1970-01-02T00:00:00"))
    val ds = TsmlReader("datasets/test/db.tsml").getDataset(ops)
    AsciiWriter.write(ds)
    //Writer("jsond").write(ds)
  }
  
  //@Test
  def select_on_non_projected_project_first {
    val ops = List(Projection("d,s"), Selection("time>=1970-01-02T00:00:00"))
    val ds = TsmlReader("datasets/test/db.tsml").getDataset(ops)
    AsciiWriter.write(ds)
  }
  
  @Test
  def select_on_non_projected_select_first {
    val ops = List(Selection("time>=1970-01-02T00:00:00"), Projection("d,s"))
    val ds = TsmlReader("datasets/test/db.tsml").getDataset(ops)
    AsciiWriter.write(ds)
  }
  
  //@Test
  def projection_in_adapter = ???
  //@Test
  def projection_order = ???
  
  //@Test
  def first_filter_in_adapter = ???
  //@Test
  def last_filter_in_adapter = ???
  //@Test
  def limit_filter_in_adapter = ???
  
  //TODO: combine, play with order
  
  //@Test
  def tsml_adapter {
    //val ds = TsmlReader("datasets/test/db.tsml").getDataset
    //ds.variables.head.asInstanceOf[Function].range
    //ds.getTimeSeries
    
    val ops = ArrayBuffer[Operation]()
    //ops += LastFilter()
    ops += FirstFilter() //NOTE: jdbc adapter doesn't preserve order of ops
    //ops += LimitFilter(2)
    ops += Selection("d>2")
    ops += Projection("i,d")
    val ds = TsmlReader("datasets/test/db.tsml").getDataset(ops)
    AsciiWriter().write(ds)
    //TODO: automate with asserts...
  }
}