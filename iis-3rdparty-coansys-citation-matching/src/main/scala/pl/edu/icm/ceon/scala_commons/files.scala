/*
 * Copyright (c) 2013-2013 ICM UW
 */

package pl.edu.icm.ceon.scala_commons

import java.io.{File, PrintWriter}

import resource._

import scala.io.{Codec, Source}

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
object files {
  def retrieveFilesByExtension(dir: File, extension: String): Iterable[File] = {
    def toFiles(f: File): Iterable[File] = {
      if (f.isDirectory)
        retrieveFilesByExtension(f, extension)
      else if (f.getName.endsWith("." + extension))
        Some(f)
      else
        None
    }
    for {
      d <- dir.listFiles()
      f <- toFiles(d)
    } yield f
  }

  class RichFile(file: File) {
    def asLines = managed(Source.fromFile(file)(Codec.UTF8))
      .acquireAndGet(source => source.getLines().toList)

    def asLines_=(ls: TraversableOnce[String]) {
      managed(new PrintWriter(file, "UTF-8"))
        .acquireAndGet(out => ls.foreach(line => out.println(line)))
    }

    def asText = asLines.mkString

    def asText_=(s: String) {
      managed(new PrintWriter(file, "UTF-8"))
        .acquireAndGet(out => out.print(s))
    }
  }


  object RichFile {
    implicit def enrichFile(file: File) = new RichFile(file)

    implicit def enrichFile(name: String) = new RichFile(new File(name))
  }

}
