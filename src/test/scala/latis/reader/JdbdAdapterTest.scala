package latis.reader

import org.junit._
import Assert._
import java.sql._
import latis.reader.tsml.TsmlReader
import latis.writer.AsciiWriter
import scala.collection.mutable.ArrayBuffer
import latis.ops._

class JdbdAdapterTest {

  private var connection: Connection = null
  
  @Before
  def makeDatabase {
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver")  //Load the JDBC driver     
    connection = DriverManager.getConnection("jdbc:derby:memory:testDB;create=true")
    
    var statement = connection.createStatement()
    statement.execute("create table test(i int, d double, s varchar(1))")
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
  
  @Test
  def tsml_adapter {
    val ops = ArrayBuffer[Operation]()
    //ops += LastFilter()
    ops += FirstFilter() //NOTE: jdbc adapter doesn't preserve order of ops
    //ops += LimitFilter(2)
    ops += Selection("d>2")
    val ds = TsmlReader("datasets/test/db.tsml").getDataset(ops)
    AsciiWriter().write(ds)
    //TODO: automate with asserts...
  }
}