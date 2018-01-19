package com.scb.common
import scala.xml.XML
object XMLParserTest {
  def main(args: Array[String]) {
   val musicElem = scala.xml.XML.loadFile("/Users/nishi/Documents/workspace/twitter-data-processor/src/main/resources/music.xml")
  
   val temp = (musicElem \\"artist" \\"album" \\ "description" \\ "@link") text
  
   print(temp)
   

   }
}