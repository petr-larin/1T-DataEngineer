import java.io.{File, FileWriter}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

trait SortTypes {
  type Keytype = Int
  type Data = ArrayBuffer[Keytype]
}

object QuickSort extends SortTypes {
  // Алгоритм из книги Мартина Одерски "Scala By Example" с добавленной
  // оптимизацией:
  // из двух партиций для первого рекурсивного вызова мы выбираем
  // меньшую, а второй рекурсивный вызов заменяем итерацией в
  // цикле while (эмуляция хвостовой рекурсии)
  def sort(xs: Data): Unit = {
    def swap(i: Int, j: Int): Unit = {
      val t = xs(i)
      xs(i) = xs(j)
      xs(j) = t
    }
    def sort1(l0: Int, r0: Int): Unit = {
      var l = l0
      var r = r0
      while (l < r) {
        val pivot = xs((l + r) / 2)
        var i = l
        var j = r
        while (i <= j) {
          while (xs(i) < pivot) i += 1
          while (xs(j) > pivot) j -= 1
          if (i <= j) {
            swap(i, j)
            i += 1
            j -= 1
          }
        }
        // в книге:
        // if (l < j) sort1(l, j)
        // if (j < r) sort1(i, r)
        // этот блок мы заменяем на:
        if (j - l < r - i){
          if (l < j) sort1(l, j)
          l = i
        } else{
          if (i < r) sort1(i, r) // в книге if (j < r) sort1(i, r) ?
          r = j
        }
      }
    }
    sort1(0, xs.length - 1)
  }
}

object HeapSort extends SortTypes {
  // https://en.wikipedia.org/wiki/Heapsort
  private def iLeftChild(i: Int) = 2 * i + 1
  //private def iRightChild(i: Int) = 2 * i + 2
  private def iParent(i: Int): Int = ((i - 1) / 2.0).floor.toInt

  private def siftDown(arr: Data, rt: Int, end: Int): Unit = {
    def swap(i: Int, j: Int): Unit = {
      val t = arr(i)
      arr(i) = arr(j)
      arr(j) = t
    }
    var root = rt
    while(iLeftChild(root) < end) {
      var child = iLeftChild(root)
      if (child + 1 < end)
        if (arr(child) < arr(child + 1))
          child += 1
      if (arr(root) < arr(child)) {
        swap(root, child)
        root = child
      }
      else return
    }
  }

  private def heapify(arr: Data): Unit = {
    var start = iParent(arr.size - 1) + 1 // Первый лист
    while(start > 0) {
      start -= 1
      siftDown(arr, start, arr.size)
    }
  }

  def sort(arr: Data): Unit = {
    def swap(i: Int, j: Int): Unit = {
      val t = arr(i)
      arr(i) = arr(j)
      arr(j) = t
    }
    heapify(arr)
    var end = arr.size
    while (end > 1) {
      end -= 1
      swap(end, 0)
      siftDown(arr, 0, end)
    }
  }
  // sort1 - это объединение предыдущих 3 функций в одну.
  // Ожидалось, что оно будет работать быстрее, но оказалось наоборот,
  // возможно из-за break
  def sort1(arr: Data): Unit = {
    def swap(i: Int, j: Int): Unit = {
      val t = arr(i)
      arr(i) = arr(j)
      arr(j) = t
    }
    var start = arr.size / 2
    var end = arr.size
    while(end > 1){
      if(start > 0) { // Построение кучи
        start -= 1
      } else{         // Извлечение из кучи
        end -= 1
        swap(end, 0)
      }
      var root = start
      breakable {
        while (iLeftChild(root) < end) {
          var child = iLeftChild(root)
          if (child + 1 < end)
            if (arr(child) < arr(child + 1))
              child += 1
          if (arr(root) < arr(child)) {
            swap(root, child)
            root = child
          }
          else break
        }
      }
    }
  }
}

object MergeSort extends SortTypes {
  // Из книги "Algorithms", авторы Sedgewick и Wayne
  // Слияние arr[lo..mid] с arr[mid+1..hi]
  private def merge(arr: Data, aux: Data, lo: Int, mid: Int, hi: Int): Unit = {
    for (k <- lo to hi) aux(k) = arr(k)
    var i = lo
    var j = mid + 1
    for (k <- lo to hi) {
      if (i > mid) {
        arr(k) = aux(j)
        j += 1
      } else if (j > hi) {
        arr(k) = aux(i)
        i += 1
      } else if (aux(j) < aux(i)) {
        arr(k) = aux(j)
        j += 1
      }
      else {
        arr(k) = aux(i); i += 1
      }
    }
  }
  // сортировка снизу вверх (bottom-up)
  def sort_bu(arr: Data): Unit = {
    var aux: Data = ArrayBuffer.fill(arr.size)(0)
    val N = arr.size
    var sz = 1
    while (sz < N) {
      var lo = 0
      while (lo < N - sz) {
        MergeSort.merge(arr, aux, lo, lo + sz - 1, Math.min(lo + sz + sz - 1, N - 1))
        lo += sz + sz
      }
      sz += sz
    }
  }
  // сортировка сверху вниз (top-down)
  def sort_td(arr: Data): Unit = {
    var aux: Data = ArrayBuffer.fill(arr.size)(0)
    def sort(lo: Int, hi: Int): Unit = {
      if (hi <= lo) return
      val mid = lo + (hi - lo)/2
      sort(lo, mid)
      sort(mid + 1, hi)
      merge(arr, aux, lo, mid, hi)
    }
    sort(0, arr.size - 1)
  }
}

object InsertionSort extends SortTypes {
  // Обычный вариант
  def sort(arr: Data): Unit = {
    for (i <- 1 until arr.size) {
      val curr = arr(i)
      var j = i - 1
      while (j >= 0 && arr(j) > curr) {
        arr(j + 1) = arr(j)
        j -= 1
      }
      arr(j + 1) = curr
    }
  }
  // Вариант с бинарным поиском места вставки
  def sort_b(arr: Data): Unit = {
    for (i <- 1 until arr.size) {
      val curr = arr(i)
      var j0 = 0
      var j1 = i - 1
      while(j0 < j1) {
        var j = (j0 + j1) / 2
      if (j == j0)
          if (arr(j + 1) > curr) j1 = j0 else j0 = j1
        else
          if (arr(j) <= curr) j0 = j else j1 = j
      }
      if (j0 == 0) if (arr(0) > curr) j0 = -1
      for (k <- i to j0 + 2 by -1) arr(k) = arr(k - 1)
      arr(j0 + 1) = curr
    }
  }
}

object Main {
  // Проверка корректности сортировки
  def validate(arr: ArrayBuffer[Int]): Boolean = {
    var check = true
    for (i <- 0 to arr.size - 2) {
      if (arr(i) > arr(i + 1)) check = false
    }
    check
  }

  // Тайминг
  // https://gist.github.com/atmb4u/21481eefd2bc1367e0a26c76b2bf5b79
  def time[R](block: => R): Double = {
    val t0 = System.nanoTime()
    block // call-by-name
    val t1 = System.nanoTime()
    val t = (t1 - t0)/1000000.0
    println("Elapsed time: %.3f ms".formatted(t))
    t
  }

  def time_all(arrLen: Int, valRange: Int, includeSlow: Boolean) = {
    def pr_info(msg: String) = println(msg, arrLen, valRange)

    val arr = ArrayBuffer.fill(arrLen) { scala.util.Random.nextInt(valRange) }
    var timings = ArrayBuffer[Double]()

    // Первый проход не учитывается (для разогрева)
    var arr_c = arr.clone()
    if(arr_c.size == 100) MergeSort.sort_bu(arr_c)

    arr_c = arr.clone()
    pr_info("MergeSort Bottom-Up")
    timings += time { MergeSort.sort_bu(arr_c) }
    println(validate(arr_c))

    arr_c = arr.clone()
    pr_info("MergeSort Top-Down")
    timings += time { MergeSort.sort_td(arr_c) }
    println(validate(arr_c))

    arr_c = arr.clone()
    pr_info("QuickSort")
    timings += time { QuickSort.sort(arr_c) }
    println(validate(arr_c))

    arr_c = arr.clone()
    pr_info("HeapSort")
    timings += time { HeapSort.sort(arr_c) }
    println(validate(arr_c))

    // медленнее, чем HeapSort.sort - не включаем
    /*
    arr_c = arr.clone()
    pr_info("HeapSort1")
    timings += time { HeapSort.sort1(arr_c) }
    println(validate(arr_c)) */

    // Не гонять медленные алгоритмы на больших объемах
    if (includeSlow) {
      arr_c = arr.clone()
      pr_info("InsertionSort")
      timings += time { InsertionSort.sort_b(arr_c) }
      println(validate(arr_c))

      arr_c = arr.clone()
      pr_info("InsertionSort with Binary Search")
      timings += time { InsertionSort.sort(arr_c) }
      println(validate(arr_c))
    }
    else timings += (-1, -1)
    timings
  }

  def main(args: Array[String]): Unit = {
    // Диапазон генерирования случайных чисел - 2 варианта:
    val valRangeL = List(10, 2_000_000_000)
    // Размеры массивов для сортировки:
    // С включением и исключением медленных алгоритмов:
    val arrLenWithSlow = List(100, 1_000, 10_000, 100_000, 1_000_000)
    val arrLenNoSlow = List(10_000_000, 100_000_000, 1_000_000_000)
    val arrLenL = List(arrLenWithSlow, arrLenNoSlow)
    val includeSlowL = List(true, false)

    for (valRange <- valRangeL) {
      for ((arrLen, includeSlow) <- arrLenL.lazyZip(includeSlowL)) {
        for (len <- arrLen) {
          println(arrLen, includeSlow)
          val t1 = time_all(len, valRange, includeSlow)
          val t2 = time_all(len, valRange, includeSlow)
          val t3 = time_all(len, valRange, includeSlow)
          val t = t1.lazyZip(t2).lazyZip(t3).map((_ + _ + _)).map(_ / 3)
          println(t)
          val fileWriter = new FileWriter(
            new File("timings_" + valRange.toString + ".csv"), true)
          fileWriter.write(len.toString)
          for (num <- t) fileWriter.write(",%.2f".formatted(num))
          fileWriter.write("\n")
          fileWriter.close()
        }
      }
    }
  }
}