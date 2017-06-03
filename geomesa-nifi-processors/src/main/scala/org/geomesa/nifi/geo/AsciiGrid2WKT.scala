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
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.gce.arcgrid.ArcGridReader
import org.geotools.process.raster.PolygonExtractionProcess

import scala.collection.JavaConversions._
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
@Tags(Array("geo", "ingest", "convert", "esri", "geotools", "ascii", "ascii grid", "grid", "WKT", "wgs84", "polygon", "raster"))
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
            IOUtils.write(csvResult, outputStream, "UTF-8")
            outputStream.close()
            getLogger.debug("written to output stream")
          }
        })
        output = session.putAttribute(output, CoreAttributes.FILENAME.key, f.getAttribute(fullFlowFileName))
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
    parse(readRaster)
  }

  private def parse(rasterData: GridCoverage2D) = {

    // reclassify (bin values of raster to -120, 0 in range 10 to shrink number of polygons
    val r1 = org.jaitools.numeric.Range.create(Integer.valueOf(-120), true, Integer.valueOf(-110), false)
    val r2 = org.jaitools.numeric.Range.create(Integer.valueOf(-110), true, Integer.valueOf(-100), false)
    val r3 = org.jaitools.numeric.Range.create(Integer.valueOf(-100), true, Integer.valueOf(-90), false)
    val r4 = org.jaitools.numeric.Range.create(Integer.valueOf(-90), true, Integer.valueOf(-80), false)
    val r5 = org.jaitools.numeric.Range.create(Integer.valueOf(-80), true, Integer.valueOf(-70), false)
    val r6 = org.jaitools.numeric.Range.create(Integer.valueOf(-70), true, Integer.valueOf(-60), false)
    val r7 = org.jaitools.numeric.Range.create(Integer.valueOf(-60), true, Integer.valueOf(-50), false)
    val r8 = org.jaitools.numeric.Range.create(Integer.valueOf(-50), true, Integer.valueOf(-40), false)
    val r9 = org.jaitools.numeric.Range.create(Integer.valueOf(-40), true, Integer.valueOf(-30), false)
    val r10 = org.jaitools.numeric.Range.create(Integer.valueOf(-30), true, Integer.valueOf(-20), false)
    val r11 = org.jaitools.numeric.Range.create(Integer.valueOf(-20), true, Integer.valueOf(-10), false)
    val r12 = org.jaitools.numeric.Range.create(Integer.valueOf(-10), true, Integer.valueOf(0), false)
    val classificationRanges = Seq(r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12)

    // store geometry as reusable WKT types
    // reverse classificationRanges to have 12 equal -120 i.e. absolutely larger values should equal larger values
    val vectorizedFeatures = extractor.execute(rasterData, 0, true, null, null, classificationRanges.reverse, null).features

    val result: collection.Seq[GeometryId] with Growable[GeometryId] = mutable.Buffer[GeometryId]()

    while (vectorizedFeatures.hasNext) {
      val vectorizedFeature = vectorizedFeatures.next()
      val geomWKTLineString = vectorizedFeature.getDefaultGeometry match {
        case g: Geometry => writer.write(g)
      }
      val dbUserData = vectorizedFeature.getAttribute(1).asInstanceOf[Double]
      result += GeometryId(dbUserData, geomWKTLineString)
    }
    result

  }

  // Abstract
  protected def fullName(f: FlowFile) = f.getAttribute("path") + f.getAttribute("filename")

  protected override def init(context: ProcessorInitializationContext): Unit = {
    relationships = Set(SuccessRelationship, FailureRelationship)
    descriptors = List[PropertyDescriptor]() // currently no configuration needed
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
