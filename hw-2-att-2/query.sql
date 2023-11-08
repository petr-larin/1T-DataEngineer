-- Часть 1: Обзор данных

-- 1. Загрузите датасет в кликхаус и проведите предварительный обзор данных. 
-- Не забудьте, что необходимо удалить те столбцы, которые в схеме не отображены. 
ALTER TABLE hw.vac 
  DROP COLUMN Professional_roles, 
  DROP COLUMN Specializations, 
  DROP COLUMN Profarea_names;
  
-- 2. Разбейте таблицу с кучей вакансий на нескольких витрин по местоположению, 
-- чтобы не нагружать одну таблицу. Внимательно выбирайте и обосновывайте выбор движка.
-- 3. Удалите те вакансии, в которых не указана заработная плата.
SELECT Area, COUNT(Area)
FROM hw.vac
GROUP BY Area;

-- В таблице всего 2 значения Area:
-- Москва	33,775
-- Санкт-Петербург	14,789
-- Создадим 2 таблицы, для Мск и СПб.
-- Заодно исключим строки без зарплаты.
CREATE TABLE IF NOT EXISTS hw.vac_msk
ENGINE = MergeTree()
ORDER BY Ids
AS SELECT
  Ids,
  Employer,
  Name,
  --Salary,
  "From",
  "To",
  Experience,
  Schedule,
  Keys,
  Description,
  --Area,
  Published_at
FROM hw.vac
WHERE Area = 'Москва' AND Salary = TRUE;

CREATE TABLE IF NOT EXISTS hw.vac_spb
ENGINE = MergeTree()
ORDER BY Ids
AS SELECT
  Ids,
  Employer,
  Name,
  --Salary,
  "From",
  "To",
  Experience,
  Schedule,
  Keys,
  Description,
  --Area,
  Published_at
FROM hw.vac
WHERE Area = 'Санкт-Петербург' AND Salary = TRUE;

DROP TABLE hw.vac;

-- Часть 2: Извлечение информации

-- 1. Напишите код для извлечения информации о средней заработной плате вакансий и местоположении, 
-- где вакансии наиболее распространены. Выведите это все в один отчет.
SELECT
  'Москва' AS "Местоположение",
  COUNT(*) AS "Кол-во вакансий",
  ROUND(AVG("From")) AS "Средняя начальная зарплата",
  ROUND(AVG("To")) AS "Средний потолок зарплаты"
FROM hw.vac_msk
  UNION ALL
SELECT
  'Санкт-Петербург' AS "Местоположение",
  COUNT(*) AS "Кол-во вакансий",
  ROUND(AVG("From")) AS "Средняя начальная зарплата",
  ROUND(AVG("To")) AS "Средний потолок зарплаты"
FROM hw.vac_spb;

-- 2. Укажите в отчете то количество строк вакансий, которое получилось после удаления.
-- Либо укажите команды, которыми вы удаляли данные.

-- в МСк 11,530 вакансий с указанием зарплаты
-- в СПб 6,052 вакансии с указанием зарплаты