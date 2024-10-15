/*
 * This file is part of CoAnSys project.
 * Copyright (c) 2012-2013 ICM-UW
 *
 * CoAnSys is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * CoAnSys is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with CoAnSys. If not, see <http://www.gnu.org/licenses/>.
 */

package pl.edu.icm.ceon.scala_commons.hadoop.writables

import java.io.DataInput
import java.io.DataOutput
import org.apache.hadoop.io.Writable

/**
 * A Writable for storing iterables of byte arrays, e.g. containing serialised protobuf objects.
 *
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
class BytesIterable(var iterable: Iterable[Array[Byte]]) extends Writable {
  def this() = this(Nil)

  def write(out: DataOutput) {
    out.writeInt(iterable.size)
    iterable foreach {
      x =>
        out.writeInt(x.length)
        out.write(x, 0, x.length)
    }
  }

  def readFields(in: DataInput) {
    val size = in.readInt()
    iterable = (1 to size).map {
      _ =>
        val len = in.readInt()
        val buff = new Array[Byte](len)
        in.readFully(buff, 0, len)
        buff
    }
  }

  override def toString =
    iterable.map(new String(_, "UTF-8")).mkString("\n")
}