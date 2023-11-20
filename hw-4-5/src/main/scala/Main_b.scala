import scala.io.StdIn.{readInt, readLine}

object Main_b {
  def main(args: Array[String]): Unit = {
    println("Годовой доход до вычета налогов: ")
    val yearlyIncomeBeforeTax = readInt()
    println("Премия в процентах от годового дохода: ")
    val bonusPercentage = readInt()
    println("Компенсация питания: ")
    val mealAllowance = readInt()
    val monthlyIncome =
      (yearlyIncomeBeforeTax * (1 + bonusPercentage/100.0)/12.0 +
        mealAllowance) * 0.87
    println(
      s"Ежемесячный оклад сотрудника после вычета налогов: $monthlyIncome")
  }
}

