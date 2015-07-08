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
import latis.time.Time
import javax.mail.Part

/**
 * Reads a folder of emails, returning the sent date, from address, subject, and content
 * of each message.
 */
class EmailAdapter(tsml: Tsml) extends IterativeAdapter[Message](tsml) {
  
  var store: Store = null
  
  override def close = {if(store != null) store.close}
    
  def getRecordIterator: Iterator[Message] = {
    val regex = """imap:\/\/([\w\d.]+):([^@]+)@([\w\d.]+)\/([\w\d\/.]+)""".r //only covers basic addresses and passwords without '@'
    val (user, password, host, folder) = getProperty("location") match {
      case Some(regex(u,p,h,f)) => (u, p, h, f)
      case None => throw new Exception("location must be formatted as 'imap://user:password@host/folder")
    }
    
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
    val vars = getOrigScalars //list of scalars from tsml
    val values = extractValues(msg)
    
    //there should be one value per scalar
    if (vars.length != values.length) {
      None //drop sample if some component of message is missing
    } else {
      val vnames: Seq[String] = vars.map(_.getName)
      val datas: Seq[Data] = (values zip vars).map(p => StringUtils.parseStringValue(p._1, p._2))
      Some((vnames zip datas).toMap)
    }
    
  }
  
  /**
   * Expects Dataset to look like: index -> (time, sender, subject, content)
   */
  def extractValues(msg: Message): Seq[String] = {
    val date = Time(msg.getSentDate).getValue.toString
    val from = msg.getFrom()(0).toString
    val subject = msg.getSubject
    
    //recursive function to find text content
    def findStringContent(p: Part): String = p.getContentType.split(";").head.split("/").head.toLowerCase match {
      case "text" => p.getContent.toString
      case "multipart" => findStringContent(p.getContent.asInstanceOf[Multipart].getBodyPart(0))
    }
    
    val content = findStringContent(msg)
    Seq(date, from, subject, content)
  }
  
}