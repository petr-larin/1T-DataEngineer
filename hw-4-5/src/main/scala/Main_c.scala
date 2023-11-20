import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object Main_c {
  def main(args: Array[String]): Unit = {

  /*
  c.Напишите программу, которая рассчитывает для каждого
    сотрудника отклонение (в процентах) от среднего значения
    оклада на уровне всего отдела. В итоговом значении
    должно учитываться в большую или меньшую сторону
    отклонение размера оклада. На вход вашей программе
    подаются все значения, аналогичные предыдущей программе,
    а также список со значениями окладов сотрудников отдела
    100, 150, 200, 80, 120, 75.
   */
    var salaries = ArrayBuffer(100, 150, 200, 80, 120, 75)
    val avgSalary = salaries.sum / salaries.length
    for (s <- salaries) {
      val delta = (s - avgSalary) * 100.0 /avgSalary
      println("Отклонение: %+.2f%%".formatted(delta))
    }
  /*
  d.Попробуйте рассчитать новую зарплату сотрудника, добавив
    (или отняв, если сотрудник плохо себя вел) необходимую
    сумму с учетом результатов прошлого задания. Добавьте его
    зарплату в список и вычислите значение самой высокой
    зарплаты и самой низкой.
   */
    val employeeIndex = 3
    val delta = (salaries(employeeIndex) - avgSalary)
    salaries(employeeIndex) -= delta
    println("Самая высокая зарплата: %d".formatted(salaries.max))
    println("Самая низкая зарплата: %d".formatted(salaries.min))
  /*
  e.Также в вашу команду пришли два специалиста с окладами 350 и
    90 тысяч рублей. Попробуйте отсортировать список сотрудников
    по уровню оклада от меньшего к большему.
   */
    salaries ++= List(350, 90)
    salaries.sortInPlace()
    println("Отсортированный список:")
    println(salaries)
  /*
  f.Кажется, вы взяли в вашу команду еще одного сотрудника и
    предложили ему оклад 130 тысяч. Вычислите самостоятельно
    номер сотрудника в списке так, чтобы сортировка не
    нарушилась, и добавьте его на это место.
   */
    val newSalary = 130
    var newIndex = 0
    breakable {
      for (s <- salaries) {
        if (s > newSalary) {
          break
        }
        newIndex += 1
      }
    }
    println(s"Индекс вставки нового сотрудника: $newIndex")
    salaries.insert(newIndex, newSalary)
    println("Список после вставки:")
    println(salaries)
  /*
  g.Попробуйте вывести номера сотрудников из полученного списка,
    которые попадают под категорию middle. На входе программе
    подается «вилка» зарплаты специалистов уровня middle.
   */
    val middle_lower = 150
    val middle_upper = 250
    var index = 0
    for (s <- salaries){
      if (middle_lower <= s && s <= middle_upper) {
        println(
          s"Номер сотрудника с зарплатой уровня middle: $index")
      }
      index += 1
    }
  /*
  h.Однако наступил кризис и ваши сотрудники требуют повысить
    зарплату. Вам необходимо проиндексировать зарплату каждого
    сотрудника на уровень инфляции  — 7 %.
   */
    salaries.mapInPlace((x) => math.round(1.07f * x))
    println("Зарплаты после индексации:")
    println(salaries)
  }
}