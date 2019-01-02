package example

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}
import java.util.zip.GZIPInputStream

import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.json4s.native.JsonMethods.parse

import scala.io.Source

object ElasticIngester {

  def getClient(hostname: String = "localhost", port: Int = 9200) = {

    new RestHighLevelClient(
      RestClient.builder(new HttpHost(hostname, port, "http")))

  }


  def processFiles(files: Seq[File]) = {

    val client = getClient()

    files.foreach(f => {

      var hasFailures = false

      val gzip = Source.fromInputStream(new GZIPInputStream(new FileInputStream(f)))

      gzip.getLines().grouped(1000).foreach(docs => {
        val start = System.currentTimeMillis
        val bulk = new BulkRequest
        docs.foreach(line => {
          var doc = line.replaceAll("\"0000-00-00\"", "null")
          doc = doc.replaceAll("undefined", "")
          val js = parse(doc)
          val id = js \ "id"
          bulk.add(new IndexRequest("profile", "doc", id.toString).source(doc, XContentType.JSON))
        })

        val result = client.bulk(bulk, RequestOptions.DEFAULT)
        println(s"Indexed ${docs.size} documents - took ${System.currentTimeMillis - start}ms - hasFailures=${result.hasFailures}, failureMessage=${if (result.hasFailures) result.buildFailureMessage()}")

        if (result.hasFailures) {
          hasFailures = true
        }
      })

     hasFailures match {
       case true =>
         println(s"File  $f has failures")
       case false => {
         val source = Paths.get(f.getPath)
         Files.move(source, source.resolveSibling(s"${f.getName}.done"))
       }
     }


    })

  }

  def processFolder(directory: String) = {
    val files = new File(directory).listFiles().filter(f=> f.getName.endsWith(".gz")).toSeq
    files.grouped(50).foreach( group => processFiles(group))
  }

}
