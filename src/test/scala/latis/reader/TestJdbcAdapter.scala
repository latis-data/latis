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
  
  @Before
  def makeDatabase {
    System.setProperty("derby.stream.error.file", "/dev/null") //don't make log file
    
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver")  //Load the JDBC driver     
    connection = DriverManager.getConnection("jdbc:derby:memory:testDB;create=true")
    
    var statement = connection.createStatement()
    statement.execute("create table test(i int, d double, s varchar(1))")
    //TODO: add other types: datetime,...
    statement.execute("insert into test values(1, 1.1, 'A')")
    statement.execute("insert into test values(2, 2.2, 'B')")
    statement.execute("insert into test values(3, 3.3, 'C')")
  }
  
  @After
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
      println(rs.getInt(1) + " " + rs.getDouble(2) + " " + rs.getString(3))
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
    val ops = List(Projection("i,s"))
    val ds = TsmlReader("datasets/test/db.tsml").getDataset(ops)
    AsciiWriter.write(ds)
  }
  
  @Test
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
    val ops = List(Selection("time>=2000-01-03"))
    val ds = TsmlReader("datasets/test/db.tsml").getDataset(ops)
    Writer("jsond").write(ds)
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