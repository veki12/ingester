package example

import java.io.FileInputStream
import java.util.zip.GZIPInputStream

import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.io.Source

object Hello extends Greeting with App {
  val f = "/home/vedran/projects/cognism/science-integration/data/0_1000.jsonl.gz"
  val gzip = Source.fromInputStream(new GZIPInputStream(new FileInputStream(f)))


  val client = new RestHighLevelClient(
    RestClient.builder(new HttpHost("localhost", 9200, "http")))


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
    println(s"Reindexed documents - took ${System.currentTimeMillis - start}ms - hasFailures=${result.hasFailures}, failureMessage=${if (result.hasFailures) result.buildFailureMessage()}")
  })


  println("Done")


}

trait Greeting {
  lazy val greeting: String = "hello"
}
