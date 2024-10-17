/*
 * Copyright (c) 2013-2013 ICM UW
 */

package pl.edu.icm.ceon.scala_commons

import annotation.tailrec

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
package object collections {
  /**
   * Similar to Array.tabulate, but here tabulated function gets partially filled array as parameter.
   *
   * @param n1 the number of elements in the 1st dimension
   * @param n2 the number of elements in the 2nd dimension
   * @param f  The function computing element values
   */
  def tabulate[T : Manifest](n1: Int, n2: Int)(f: (Array[Array[T]], Int, Int) => T): Array[Array[T]] = {
    val opt = Array.ofDim[T](n1, n2)

    for (i <- 0 until n1) {
      for (j <- 0 until n2)
        opt(i)(j) = f(opt, i, j)
    }

    opt
  }

  /**
   * Generates a list of specified length.
   *
   * @param el the element the list should contain
   * @param count the length of the list
   */
  def generate[T](el: T, count: Int): List[T] =
    extended(el, count, Nil)

  /**
   * Extends the baseList by appending at its begining cout elements el.
   *
   * @param el the element to be appended
   * @param count the number of elements to append
   * @param baseList the list to be extended
   */
  def extended[T](el: T, count: Int, baseList: List[T]): List[T] =
    if (count > 0)
      extended(el, count - 1, el :: baseList)
    else
      baseList

  /**
   * Returns a list containing all the elems separated by newEl.
   *
   * E.g.:
   * insert(Nil, 0) == Nil
   * insert(List(1), 0) == List(1)
   * insert(List(1,2,3), 0) == List(1,0,2,0,3)
   */
  def insert[T](elems: List[T], newEl: T): List[T] = {
    @tailrec
    def helper(elems: List[T], newEl: T, acc: List[T]): List[T] = {
      if (elems.isEmpty)
        acc.reverse
      else if (elems.tail.isEmpty)
        helper(elems.tail, newEl, elems.head :: acc)
      else
        helper(elems.tail, newEl, newEl :: elems.head :: acc)
    }
    helper(elems, newEl, Nil)
  }

  /**
   * Creates a boolean list of given size having true at indices specified in elems and false otherwise.
   */
  def characteristicList(elems: Traversable[Int], size: Int): List[Boolean] = {
    val diffs = (-1 +: elems.toSeq :+ size).sliding(2).map(x => x(1) - x(0))
    val falses = diffs.map(x => generate(false, x - 1)).toList

    insert(falses, List(true)).flatten
  }

  /**
   * Splits a list on elements satisfying a predicate and removes these elements.
   */
  def split[T](elems: List[T])(pred: T => Boolean) = {
    val reversed = elems.foldLeft(List(List.empty[T])) {
      case (Nil, _) =>
        throw new RuntimeException
      case (h :: t, el) =>
        if (pred(el))
          Nil :: h :: t
        else
          (el :: h) :: t
    }
    reversed.filterNot(_.isEmpty).map(_.reverse).reverse
  }


  /**
   * Returns all 2-element subsets.
   */
  def unorderedPairs[A](list: List[A]): Iterator[(A,A)] = for {
    h1::t1 <- list.tails
    h2::t2 <- t1.tails
  } yield (h1, h2)


  def sortedPairs[A: Ordering](list: List[A]): Iterator[(A,A)] = {
    def sortPair(pair: (A, A)): (A, A) =
      if (implicitly[Ordering[A]].lt(pair._1, pair._2))
        pair
      else
        pair.swap

    unorderedPairs(list).map(sortPair)
  }

  def sortedTriples[A: Ordering](list: List[A]): Iterator[(A,A,A)] = for {
    h1::t1 <- list.sorted.tails
    h2::t2 <- t1.tails
    h3::t3 <- t2.tails
  } yield (h1, h2, h3)


  /**
   * Removes the first element statisfying the predicate.
   */
  def excludeOne[A](list: List[A], pred: A => Boolean): List[A] = {
    @tailrec
    def helper(list: List[A], pred: A => Boolean, acc: List[A]): List[A] = list match {
      case Nil => acc.reverse
      case h::t =>
        if (pred(h))
          acc.reverse ++ t
        else
          helper(t, pred, h::acc)
    }

    helper(list, pred, Nil)
  }
}
