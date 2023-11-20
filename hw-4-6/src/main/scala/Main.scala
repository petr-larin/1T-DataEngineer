import scala.collection.mutable.ArrayBuffer

object Main {
  type PascalTriangle = ArrayBuffer[ArrayBuffer[Int]];

  /*
     Создайте функцию на Scala, которая генерирует треугольник Паскаля до
     заданного уровня. Треугольник Паскаля представляется как список списков,
     где каждый список содержит числа для соответствующего уровня треугольника.
   */
  def buildPascalTriangle(size: Int): PascalTriangle = {

    val triangle = ArrayBuffer[ArrayBuffer[Int]]()
    triangle += ArrayBuffer[Int](1)

    var row = 1;
    while(row < size){
      val prevLine1 = 0 +: triangle(row - 1)
      val prevLine2 = triangle(row - 1) :+ 0
      val newLine = prevLine1.zip(prevLine2).map { case (x, y) => x + y }
      triangle += newLine
      row += 1
    }
    triangle
  }

  /*
     Напишите функцию, которая принимает сгенерированный треугольник
     Паскаля и выводит его содержимое в удобном формате.
   */
  def printPascalTriangle(pascalTriangle: PascalTriangle) : Unit = {
    for (line <- pascalTriangle) {
      var str = ""
      for (v <- line) str += "%7s".formatted(v)
      val pad_len = 60 - str.length / 2
      print(" " * pad_len)
      println(str)
    }
  }

  /*
     Реализуйте функцию, которая принимает номер строки и номер элемента
     в этой строке треугольника Паскаля и возвращает значение этого элемента.
   */
  def lookUpPascalTriangle(pascalTriangle: PascalTriangle, row: Int, col: Int): Int =
    pascalTriangle(row)(col)

  /*
     Создайте функцию, которая находит сумму элементов в указанной строке
     треугольника Паскаля.
   */
  def sumPascalTriangle(pascalTriangle: PascalTriangle, row: Int): Int =
    pascalTriangle(row).sum
  /*
     Реализуйте функцию, которая проверяет, является ли треугольник Паскаля
     симметричным (то есть, числа симметричны относительно вертикальной оси).
   */

  def isSymmetrical(pascalTriangle: PascalTriangle): Boolean =
    pascalTriangle.map((x) => x == x.reverse).foldLeft(true)((x, y) => x && y)

  def main(args: Array[String]): Unit = {
    val triangle = buildPascalTriangle(12)
    printPascalTriangle(triangle)
    println(lookUpPascalTriangle(triangle, 5, 2))
    println(sumPascalTriangle(triangle, 6))
    println(isSymmetrical(triangle))
  }
}