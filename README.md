# Medallion_Architecture

###### Фінансовий аналітичний пайплайн за архітектурою Medallion (Bronze–Silver–Gold) у середовищі Databricks, використовуючи дані з Yahoo Finance через бібліотеку yfinance.

###### Мета проекту
Дата-пайплайн для фінансових даних з використанням концепції Medallion Architecture:
•	Bronze Layer – сирі дані з Yahoo Finance
•	Silver Layer – очищені та структуровані дані
•	Gold Layer – агрегована інформація для бізнес-аналітики

1.	**Bronze Layer** - Завантаження сирих фінансових даних з Yahoo Finance для компаній Industries in Software – Infrastructure Sector:
•	MSFT Microsoft Corporation
•	NVDA NVIDIA Corporation
•	AAPL Apple Inc.
•	ORCL Oracle Corporation
•	IBM International Business Machines Corporation
_0_script_YahooFinance_for_Databricks.ipynb_
_1_script_Bronze_Layer.ipynb_
Дані отримано з Yahoo Finance через бібліотеку yfinance, використовуючи Notebook Datalore (у середовищі Databricks community з Yahoo Finance
отримання через yfinance неможливе - блокування через надмірну кількість запитів), та збережено в файл .csv для подальшого завантаження в
Databricks >> Catalog
Завантаження даних (spark.read.format("csv")), форматування назв колонок (заміна пробілів та спецсимволів).
Збереження даних в Delta Table для сирих даних - saveAsTable("**yahoo_data.finance_bronze**")

2.	**Silver Layer** - Очищення та підготовка даних.
_2_script_Silver_Layer.ipynb_
Обробка даних для Silver Layer, вивантажених з yahoo_data.finance_bronze:
•	Очищення NULL-значень
•	Виправлення колонок
•	Видалення дублікатів
•	Форматування дат
Збереження даних в Delta Table - saveAsTable("**yahoo_data.finance_silver**")

3.	**Gold Layer** - Створення агрегованих таблиць з ключовими фінансовими метриками.
_3_script_Gold_Layer.ipynb_ 
Агрегація метрик по днях і компанії, вивантажених з yahoo_data.finance_silver:
•	Середня ціна акцій (Avg_Close)
•	Максимальна та мінімальна ціна (Max_High, Min_Low)
•	Загальний обсяг торгів (Total_Volume)
•	Денна зміна ціни (Price_Change)
Збереження даних в Delta Table - saveAsTable("**yahoo_data.finance_gold**")
  
4. Бізнес-цінність кожного шару та приклади використання.
**Bronze Layer**
 - Збереження повної історії неочищених даних — можна використовувати для аудитів, відстеження змін, для навчання моделей без попередньої обробки.
**Silver Layer**
 - Якість даних — це вже підготовлені дані для звітності: видалені помилки, дублювання та відкориговані формати - легша обробка для BI-рішень та SQL-запитів
**Gold Layer**
 - Пряме використання для ухвалення рішень — агреговані метрики готові до використання в аналітичних дашбордах та фінансових звітах.
 - Швидкий доступ до показників — дозволяє швидко отримати ключові фінансові метрики без складних розрахунків.
 - Прогнозування та стратегічне планування — основа для розробки аналітики трендів ринку.
