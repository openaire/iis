/*
 * This file is part of CoAnSys project.
 * Copyright (c) 2012-2015 ICM-UW
 *
 * CoAnSys is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * CoAnSys is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with CoAnSys. If not, see <http://www.gnu.org/licenses/>.
 */

package pl.edu.icm.coansys.citations.data

import java.io.{DataOutput, DataInput}
import org.apache.hadoop.io.{BytesWritable, WritableComparable, Text}

class TextWithBytesWritable (val text: Text, val bytes: BytesWritable) extends WritableComparable[TextWithBytesWritable] {
  def this() = this(new Text(), new BytesWritable())
  def this(text: String, bytes: Array[Byte]) = this(new Text(text), new BytesWritable(bytes))

  def readFields(in: DataInput) {
    text.readFields(in)
    bytes.readFields(in)
  }

  def write(out: DataOutput) {
    text.write(out)
    bytes.write(out)
  }

  def compareTo(o: TextWithBytesWritable): Int = text.compareTo(o.text)

  override def hashCode(): Int = text.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case o: TextWithBytesWritable =>
      text.equals(o.text)
    case _ => false
  }

  override def toString: String = text.toString
}