SELECT YEAR, regname as Region, MAX(HistBall100) AS MaxMark FROM zno_data
WHERE HistTestStatus = 'Зараховано'
GROUP BY regname, year