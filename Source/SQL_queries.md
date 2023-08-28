# Cycle Temperature

```sql
SELECT
  CycleNumber,
  concat(
    '<div style="background-color:#',
    CASE
      WHEN format_number(temperature, 0) BETWEEN 0
      AND 9 THEN 'FFFB10'
      WHEN temperature BETWEEN 9
      AND 15 THEN 'FFB710'
      WHEN temperature BETWEEN 15
      AND 80 THEN 'FF4A10'
      ELSE '10C5FF'
    END,
    ';  text-align:center;"> ',
    format_number(temperature, 0),
    '</div>'
  ) AS `Cycle Temperature`
FROM
  (
    SELECT
      CycleNumber,
      First(CycleTemperatue_Max) AS temperature
    FROM
      `lion`.`gold_nasa_charge_cycles`
    GROUP BY
      CycleNumber
    HAVING
      temperature > 0
  )
ORDER BY
  CycleNumber
LIMIT
  400
```

```sql
SELECT
  CycleNumber,
  concat(
    '<div style="background-color:#',
    CASE
      WHEN format_number(temperature, 0) BETWEEN 0
      AND 9 THEN 'FFFB10'
      WHEN temperature BETWEEN 9
      AND 15 THEN 'FFB710'
      WHEN temperature BETWEEN 15
      AND 80 THEN 'FF4A10'
      ELSE '10C5FF'
    END,
    ';  text-align:center;"> ',
    format_number(temperature, 0),
    '</div>'
  ) AS `Cycle Temperature`
FROM
  (
    SELECT
      CycleNumber,
      First(CycleTemperatue_Max) AS temperature
    FROM
      `lion`.`gold_nasa_charge_cycles`
    GROUP BY
      CycleNumber
    HAVING
      temperature > 0
  )
ORDER BY
  CycleNumber
LIMIT
  400
```

# Stanford Gold

```sql
SELECT CycleNumber AS Id, CycleCurrent_Median AS Current, CycleVoltage_Median AS Voltage, CycleTemperatue_Median AS Temperature, CycleDuration AS Duration, Manufacture FROM lion.gold_stf_NMC_discharge_cycles
```

# NASA Gold
```sql
SELECT CycleNumber AS Id, CycleCurrent_Median AS Current, CycleVoltage_Median AS Voltage, CycleTemperatue_Median AS Temperature, CycleDuration AS Duration, Profile FROM lion.gold_nasa_charge_cycles
```

# Counts

```sql
SELECT
  DISTINCT(CycleNumber)
FROM
  `lion`.`gold_stf_nmc_discharge_cycles`
```
