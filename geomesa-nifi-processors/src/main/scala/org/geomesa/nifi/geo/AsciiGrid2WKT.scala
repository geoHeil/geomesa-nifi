/** *********************************************************************
  * Copyright (c) 2017 @geheil
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  * **********************************************************************/


package org.geomesa.nifi.geo

import java.io.{IOException, InputStream, OutputStream}

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTWriter
import org.apache.commons.io.IOUtils
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.{OnRemoved, OnScheduled}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.flowfile.attributes.CoreAttributes
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.OutputStreamCallback
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.Relationships._
import org.geotools.gce.arcgrid.ArcGridReader
import org.geotools.process.raster.PolygonExtractionProcess

import scala.collection.JavaConverters._
import scala.collection.generic.Growable
import scala.collection.mutable

/**
  * DTO object class, TODO consider simple feature
  *
  * @param value user data value for each raster point
  * @param geom  parsed WKT WGS-84 geometry linestring
  */
// removed path as this is kept in the flow-files attributes
case class GeometryId(/*inputPath: String,*/ value: Double, geom: String)

/**
  * Based on geotools arcgrid support http://docs.geotools.org/latest/userguide/library/coverage/arcgrid.html convert
  * ascii grid files to WKT and metadata representation
  */
@Tags(Array("geo", "ingest", "convert", "esri", "geotools", "ascii", "ascii grid", "grid", "WKT", "wgs84"))
@CapabilityDescription("Convert ESRi ASCII grid files to WKT")
class AsciiGrid2WKT extends AbstractProcessor {

  type ProcessFn = (ProcessContext, ProcessSession, FlowFile) => Unit

  private var descriptors: java.util.List[PropertyDescriptor] = null
  private var relationships: java.util.Set[Relationship] = null
  @volatile
  private var extractor: PolygonExtractionProcess = null
  @volatile
  private var writer: WKTWriter = null

  override def getRelationships = relationships

  override def getSupportedPropertyDescriptors = descriptors

  @OnRemoved
  def cleanup(): Unit = {
    getLogger.info("Shut down AsciiGrid2WKT processor " + getIdentifier)
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit =
    Option(session.get()).foreach { f =>
      try {
        getLogger.info(s"Processing file ${fullName(f)}")
        val in = session.read(f)
        val fullFlowFileName = fullName(f)

        val result: Seq[GeometryId] with Growable[GeometryId] = parsAsciiGridtoWKT(in)

        val csvResult: String = serializeCSV(result)

        var output = session.write(f, new OutputStreamCallback() {
          @throws[IOException]
          def process(outputStream: OutputStream): Unit = {
            getLogger.debug(s"created csv  ${csvResult}")
            IOUtils.write(csvResult, outputStream, "UTF-8")
            outputStream.close()
            getLogger.debug("written to output stream")
          }
        })
        output = session.putAttribute(output, CoreAttributes.FILENAME.key, f.getAttribute(CoreAttributes.FILENAME.key()))
        session.transfer(output, SuccessRelationship)
      } catch {
        case e: Exception =>
          getLogger.error(s"Error: ${e.getMessage}", e)
          session.transfer(f, FailureRelationship)
      }
    }

  private def serializeCSV(result: Seq[GeometryId] with Growable[GeometryId]) = {
    // https://stackoverflow.com/questions/30271823/converting-a-case-class-to-csv-in-scala
    // TODO use avro, manually serialize to CSV
    val lineSep = System.getProperty("line.separator")
    val csvResult = result.map(p => p.productIterator.map {
      case Some(value) => value
      case None => ""
      case rest => rest
    }.mkString(";")).mkString(lineSep)
    csvResult
  }

  /**
    * perform conversion from ascii grid to WKT polygons
    *
    * @param in flowfiles's input stream
    * @return vectorized polygons and userdata, filename kept as attribute
    */
  private def parsAsciiGridtoWKT(in: InputStream) = {
    val readRaster = new ArcGridReader(in).read(null)
    val vectorizedFeatures = extractor.execute(readRaster, 0, true, null, null, null, null).features
    val result: Seq[GeometryId] with Growable[GeometryId] = mutable.Buffer[GeometryId]()
    while (vectorizedFeatures.hasNext) {
      val vectorizedFeature = vectorizedFeatures.next()
      val geomWKTLineString = vectorizedFeature.getDefaultGeometry match {
        case g: Geometry => writer.write(g)
      }
      val userData = vectorizedFeature.getAttribute(1).asInstanceOf[Double]

      // TODO store path name as attribute
      result += GeometryId(/*fullFlowFileName, */ userData, geomWKTLineString)
    }
    result
  }

  // Abstract
  protected def fullName(f: FlowFile) = f.getAttribute("path") + f.getAttribute("filename")

  //  protected def converterIngester(): ProcessFn =
  //    (context: ProcessContext, session: ProcessSession, flowFile: FlowFile) => {
  //      getLogger.debug("Running converter based ingest")
  //      val fullFlowFileName = fullName(flowFile)
  //
  //      session.read(flowFile, new InputStreamCallback {
  //        override def process(in: InputStream): Unit = {
  //          val readRaster = new ArcGridReader(in).read(null)
  //          val vectorizedFeatures = extractor.execute(readRaster, 0, true, null, null, null, null).features
  //          val result: collection.Seq[GeometryId] with Growable[GeometryId] = mutable.Buffer[GeometryId]()
  //          while (vectorizedFeatures.hasNext) {
  //            // TODO rewrite in idiomatic scala?
  //            val vectorizedFeature = vectorizedFeatures.next()
  //            val geomWKTLineString = vectorizedFeature.getDefaultGeometry match {
  //              case g: Geometry => writer.write(g)
  //            }
  //            val userData = vectorizedFeature.getAttribute(1).asInstanceOf[Double]
  //
  //            // TODO store path name as attribute
  //            result += GeometryId(fullFlowFileName, userData, geomWKTLineString)
  //          }
  //
  //          //TODO: May want to have a better default name....
  //          //          output = session.putAttribute(output, CoreAttributes.FILENAME.key, UUID.randomUUID.toString + ".json")
  //          //          session.transfer(result, SuccessRelationship)
  //        }
  //      })
  //      getLogger.debug(s"Converted file $fullFlowFileName")
  //    }

  protected override def init(context: ProcessorInitializationContext): Unit = {
    relationships = Set(SuccessRelationship, FailureRelationship).asJava
    descriptors = List[PropertyDescriptor]().asJava // currently no configuration needed
  }

  @OnScheduled
  protected def initialize(context: ProcessContext): Unit = {

    extractor = new PolygonExtractionProcess()

    writer = new WKTWriter()

    getLogger.info(s"Initialized geotools")
  }

}

object AsciiGrid2WKT {

  object Properties {

  }

  object Relationships {
    final val SuccessRelationship = new Relationship.Builder().name("success").description("Success").build
    final val FailureRelationship = new Relationship.Builder().name("failure").description("Failure").build
  }

}
