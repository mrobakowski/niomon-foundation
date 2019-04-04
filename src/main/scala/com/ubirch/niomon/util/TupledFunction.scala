package com.ubirch.niomon.util

trait TupledFunction[F] {
  type TupledInput
  type Output

  final type TupledF = TupledInput => Output

  def tupled(f: F): TupledF

  def untupled(tf: TupledF): F
}

//noinspection TypeAnnotation
object TupledFunction {

  implicit def tupledFunction0[R] = new TupledFunction[() => R] {
    override type TupledInput = Unit
    override type Output = R

    override def tupled(f: () => R): TupledF = { _: Unit => f() }

    override def untupled(tf: TupledF): () => R = { () => tf(()) }
  }

  implicit def tupledFunction1[T, R] = new TupledFunction[T => R] {
    override type TupledInput = T
    override type Output = R

    override def tupled(f: T => R): TupledF = f

    override def untupled(tf: TupledF): T => R = tf
  }

  implicit def tupledFunction2[A, B, R] = new TupledFunction[(A, B) => R] {
    override type TupledInput = (A, B)
    override type Output = R

    override def tupled(f: (A, B) => R): TupledF = f.tupled

    override def untupled(tf: TupledF): (A, B) => R = { (a, b) => tf((a, b)) }
  }

  implicit def tupledFunction3[A, B, C, R] = new TupledFunction[(A, B, C) => R] {
    override type TupledInput = (A, B, C)
    override type Output = R

    override def tupled(f: (A, B, C) => R): TupledF = f.tupled

    override def untupled(tf: TupledF): (A, B, C) => R = { (a, b, c) => tf((a, b, c)) }
  }

  implicit def tupledFunction4[A, B, C, D, R] = new TupledFunction[(A, B, C, D) => R] {
    override type TupledInput = (A, B, C, D)
    override type Output = R

    override def tupled(f: (A, B, C, D) => R): TupledF = f.tupled

    override def untupled(tf: TupledF): (A, B, C, D) => R = { (a, b, c, d) => tf((a, b, c, d)) }
  }

}
