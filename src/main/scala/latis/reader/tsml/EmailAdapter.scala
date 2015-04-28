package latis.reader.tsml

import latis.reader.tsml.ml.Tsml
import javax.mail.Message
import java.util.Properties
import javax.mail.Session
import javax.mail.Store
import javax.mail.Folder
import latis.data.Data
import latis.dm._
import latis.util.StringUtils
import java.text.DateFormat
import java.text.SimpleDateFormat
import javax.mail.Multipart

class EmailAdapter(tsml: Tsml) extends IterativeAdapter[Message](tsml) {
  
  var store: Store = null
  
  override def close = {if(store != null) store.close}
  
  val host = getProperty("host").getOrElse(throw new Exception("EmailAdapter requires 'host' property defined in tsml"))
  val user = getProperty("user").getOrElse(throw new Exception("EmailAdapter requires 'user' property defined in tsml"))
  val password = getProperty("password").getOrElse(throw new Exception("EmailAdapter requires 'password' property defined in tsml"))
  val folder = getProperty("folder").getOrElse(throw new Exception("EmailAdapter requires 'folder' property defined in tsml"))
  
  def getRecordIterator: Iterator[Message] = {
    val props = new Properties
    props.setProperty("mail.store.protocol", "imaps")
    val session: Session = Session.getInstance(props, null)
    store = session.getStore
    store.connect(host, user, password)
    val inbox: Folder = store.getFolder(folder)
    inbox.open(Folder.READ_ONLY)
    inbox.getMessages.toIterator
  }
  
  def parseRecord(msg: Message): Option[Map[String, Data]] = {
    val vars = getOrigScalars
    val values = extractValues(msg)
    
    if (vars.length != values.length) {
      None
    } else {
      val vnames: Seq[String] = vars.map(_.getName)
      val datas: Seq[Data] = (values zip vars).map(p => Data(StringUtils.padOrTruncate(p._1, p._2)))
      Some((vnames zip datas).toMap)
    }
    
  }
  
  def extractValues(msg: Message): Seq[String] = {
    val format = getOrigDataset.findVariableByName("time").get.getMetadata("units").getOrElse("yyyy/MM/dd")
    val date = (new SimpleDateFormat(format)).format(msg.getSentDate)
    val from = msg.getFrom()(0).toString
    val subject = msg.getSubject
    val content = msg.getContent.asInstanceOf[Multipart].getBodyPart(0).getContent.toString
    Seq(date, from, subject, content)
  }
  
}