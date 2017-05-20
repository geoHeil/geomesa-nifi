/** *********************************************************************
  * Copyright (c) 2017 @geheil
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  * **********************************************************************/


package org.geomesa.nifi.geo

import java.io.InputStream

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTWriter
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.{OnRemoved, OnScheduled}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.geomesa.nifi.geo.AbstractGeoIngestProcessor.Relationships._
import org.geotools.gce.arcgrid.ArcGridReader
import org.geotools.process.raster.PolygonExtractionProcess

import scala.collection.JavaConverters._
import scala.collection.generic.Growable
import scala.collection.mutable

/**
  * DTO object class, TODO consider simple feature
  *
  * @param value     user data value for each raster point
  * @param inputPath stores original file name
  * @param geom      parsed WKT WGS-84 geometry linestring
  */
case class GeometryId(inputPath: String, value: Double, geom: String)

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
        try {
          val fn: ProcessFn = converterIngester()
          fn(context, session, f)
        } finally {
          // TODO close resources and write to file
        }
        session.transfer(f, SuccessRelationship)
      } catch {
        case e: Exception =>
          getLogger.error(s"Error: ${e.getMessage}", e)
          session.transfer(f, FailureRelationship)
      }
    }

  protected def converterIngester(): ProcessFn =
    (context: ProcessContext, session: ProcessSession, flowFile: FlowFile) => {
      getLogger.debug("Running converter based ingest")
      val fullFlowFileName = fullName(flowFile)

      session.read(flowFile, new InputStreamCallback {
        override def process(in: InputStream): Unit = {
          getLogger.info(s"Converting path $fullFlowFileName")

          val readRaster = new ArcGridReader(in).read(null)
          val vectorizedFeatures = extractor.execute(readRaster, 0, true, null, null, null, null).features
          val result: collection.Seq[GeometryId] with Growable[GeometryId] = mutable.Buffer[GeometryId]()
          while (vectorizedFeatures.hasNext) {
            // TODO rewrite in idiomatic scala?
            val vectorizedFeature = vectorizedFeatures.next()
            val geomWKTLineString = vectorizedFeature.getDefaultGeometry match {
              case g: Geometry => writer.write(g)
            }
            val userData = vectorizedFeature.getAttribute(1).asInstanceOf[Double]

            result += GeometryId(fullFlowFileName, userData, geomWKTLineString)
          }
          //TODO store result
          //flowFile = session.putAttribute(flowFile, "match", "completed")
          result
        }
      })
      getLogger.debug(s"Converted file $fullFlowFileName")
    }



  // Abstract
  protected def fullName(f: FlowFile) = f.getAttribute("path") + f.getAttribute("filename")

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

object AbstractGeoIngestProcessor {

  object Properties {

  }

  object Relationships {
    final val SuccessRelationship = new Relationship.Builder().name("success").description("Success").build
    final val FailureRelationship = new Relationship.Builder().name("failure").description("Failure").build
  }

}
