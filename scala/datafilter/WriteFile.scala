package scala.datafilter

import java.io._
import java.text.SimpleDateFormat
import java.util.Date


/**
  * Created by pi on 17-7-30.
  */
class WriteFile {

  def write(path:String,file:String,s:String)={
    val writer = new PrintWriter(new File(path+file+getNowDate()))

    writer.write(s)
    writer.close()
  }

  def append(path:String,content:String)={

      // Specify the file name and path here
      val file = new File(path)
      // This logic is to create the file if the file is not already present
      if (!file.exists()) {
        file.createNewFile()
      }
      // Here true is to append the content to the file
      val fw = new FileWriter(file, true)
      // BufferedWriter writer give better performance
      val bw = new BufferedWriter(fw)
      bw.write(content)
      // closing BufferedWriter Stream
      bw.close()
      //System.out.println("Data successfully appended at the end of file.");

  }

  def getNowDate():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var hehe = dateFormat.format( now )
    hehe
  }

}
