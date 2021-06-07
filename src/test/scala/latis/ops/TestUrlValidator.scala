package latis.ops

import org.junit._
import org.junit.Assert._
import requests._

import latis.dm._
import latis.metadata.Metadata

class TestUrlValidator {

  @Test
  def valid_url(): Unit = {
    assertEquals(200, requests.head("http://google.com").statusCode)
  }

  @Test(expected=classOf[RequestFailedException])
  def invalid_url(): Unit = {
    requests.head("http://github.com/latis-data/nope")
    fail("Should have thrown RequestFailedException")
  }

  @Test
  def valid_sample(): Unit = {
    val sample = Sample(Text(Metadata("time"), "1"), Text(Metadata("url"), "http://google.com"))
    assertTrue(UrlValidator().applyToSample(sample).nonEmpty)
  }

  @Test
  def invalid_sample(): Unit = {
    val sample = Sample(Text(Metadata("time"), "1"), Text(Metadata("url"), "http://github.com/latis-data/nope"))
    assertTrue(UrlValidator().applyToSample(sample).isEmpty)
  }
}
