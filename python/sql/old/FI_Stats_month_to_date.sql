DECLARE  @MTD table( SalesThisYear float,GrossThisYear float,SalesLastYear float,GrossLastYear float,SalesThisMonth float,GrossThisMonth float,SalesLastMonth float,GrossLastMonth float,InvoicesThisMonth int,InvoicesLastMonth int,CreditNotesThisMonth int,CreditNotesLastMonth int,InvoicesThisYear int,InvoicesLastYear int,CreditNotesThisYear int,CreditNotesLastYear int,[Days] Datetime)
DECLARE @Y int =2024;
WHILE @Y <= 2024
BEGIN
DECLARE @Period INT = 1,@Year int =@Y;
WHILE @Period < 13
BEGIN
declare @FirstDay datetime, @LastDay datetime
set @FirstDay = convert(datetime,convert(varchar,@Period) +'/1/'+ convert(varchar,@Year))
set @LastDay= eomonth(convert(datetime,convert(varchar,@Period) +'/1/'+ convert(varchar,@Year)))
--DECLARE @Today DATETIME = CAST(CAST(GETDATE() AS DATE) AS DATETIME)
DECLARE @Today DATETIME =eomonth(convert(datetime,convert(varchar,@Period) +'/1/'+ convert(varchar,@Year)))
DECLARE @SameDayLastWeek DATETIME = DATEADD(DD, -7,  @Today)
DECLARE @SameDayLastMonth DATETIME = DATEADD(MONTH, -1,  @Today)
DECLARE @SameDayLastQuarter DATETIME = DATEADD(QQ, -1, @Today)
DECLARE @SameDayLastYear DATETIME = DATEADD(YEAR, -1, @Today)
DECLARE @StartOfCurrentMonth DATETIME = DATEADD(MONTH, DATEDIFF(MONTH,0,@Today), 0)
DECLARE @StartOfPreviousMonth DATETIME = DATEADD(MONTH, DATEDIFF(MONTH,0, @Today)-1,0)
DECLARE @StartOfCurrentQuarter DATETIME = DATEADD(QQ, DATEDIFF(QQ,0, @Today),0)
DECLARE @StartOfPreviousQuarter DATETIME = DATEADD(QQ, DATEDIFF(QQ,0, @Today)-1,0)
DECLARE @StartOfCurrentYear DATETIME = CAST(YEAR(@Today) AS NVARCHAR(4))+'0101'
DECLARE @StartOfPreviousYear DATETIME = CAST(YEAR(@Today)-1 AS NVARCHAR(4))+'0101'
DECLARE @EndOfCurrentYear DATETIME = CAST(YEAR(@Today) AS NVARCHAR(4))+'1231'
DECLARE @EndOfPreviousYear DATETIME = CAST(YEAR(@Today)-1 AS NVARCHAR(4))+'1231'
DECLARE @EndOfCurrentMonth DATETIME = DATEADD(S, -1, DATEADD(MONTH, DATEDIFF(MONTH,0,@Today)+1,0))
DECLARE @EndOfPreviousMonth DATETIME = DATEADD(S,-1, DATEADD(MONTH, DATEDIFF(MONTH,0, @Today),0))
DECLARE @EndOfCurrentQuarter DATETIME = DATEADD(S,-1, DATEADD(QQ, DATEDIFF(QQ,0, @Today)+1,0))
DECLARE @EndOfPreviousQuarter DATETIME = DATEADD(S,-1, DATEADD(QQ, DATEDIFF(QQ,0, @Today),0))

DECLARE @CurrentFinanceYear DATETIME = (select MAX(FinancYear) as financYearStart from OACP where FinancYear<= @Today)
DECLARE @PreviousFinanceYear DATETIME = (select MAX(FinancYear) as financYearStart from OACP where FinancYear< @CurrentFinanceYear)
DECLARE @StartOfCurrentFinancialYear DATETIME = (SELECT MIN(T1.F_RefDate) FROM OACP T0 JOIN OFPR T1 ON T0.PeriodCat = T1.Category WHERE T0.FinancYear = @CurrentFinanceYear)
DECLARE @StartOfPreviousFinancialYear DATETIME = (SELECT MIN(T1.F_RefDate) FROM OACP T0 JOIN OFPR T1 ON T0.PeriodCat = T1.Category WHERE T0.FinancYear = @PreviousFinanceYear)
DECLARE @EndOFCurrenctFinancialYear DATETIME = (SELECT MAX(T1.T_RefDate) FROM OACP T0 JOIN OFPR T1 ON T0.PeriodCat = T1.Category WHERE T0.FinancYear = @CurrentFinanceYear)
DECLARE @EndOfPreviousFinancialYear DATETIME = (SELECT MAX(T1.T_RefDate) FROM OACP T0 JOIN OFPR T1 ON T0.PeriodCat = T1.Category WHERE T0.FinancYear = @PreviousFinanceYear)

DECLARE @CurrentStart DATETIME = @StartOfCurrentFinancialYear
DECLARE @CurrentEnd DATETIME = @Today
DECLARE @PreviousStart DATETIME = @StartOfPreviousFinancialYear
DECLARE @PreviousEnd DATETIME = @SameDayLastYear
insert into @MTD
SELECT
(
(SELECT ISNULL(SUM(CASE T0.CANCELED WHEN 'C' THEN -(T0.DocTotal-T0.VatSum+T0.DpmAmnt-T0.TotalExpns-T0.RoundDif) ELSE (T0.DocTotal-T0.VatSum+T0.DpmAmnt-T0.TotalExpns-T0.RoundDif) END),0) FROM OINV T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @CurrentStart AND @CurrentEnd)
-
(SELECT ISNULL(SUM(CASE T0.CANCELED WHEN 'C' THEN -(T0.DocTotal-T0.VatSum+T0.DpmAmnt-T0.TotalExpns-T0.RoundDif) ELSE (T0.DocTotal-T0.VatSum+T0.DpmAmnt-T0.TotalExpns-T0.RoundDif) END),0) FROM ORIN T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @CurrentStart AND @CurrentEnd
AND NOT EXISTS (SELECT 1 FROM RIN1 TS1 WHERE TS1.DocEntry = T0.DocEntry AND TS1.BaseType = 203))
) AS 'SalesThisYear',
(
(SELECT ISNULL(SUM(CASE T0.CANCELED WHEN 'C' THEN -T0.GrosProfit ELSE T0.GrosProfit END),0) FROM OINV T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @CurrentStart AND @CurrentEnd)
-
(SELECT ISNULL(SUM(CASE T0.CANCELED WHEN 'C' THEN -T0.GrosProfit ELSE T0.GrosProfit END),0) FROM ORIN T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @CurrentStart AND @CurrentEnd
AND NOT EXISTS (SELECT 1 FROM RIN1 TS1 WHERE TS1.DocEntry = T0.DocEntry AND TS1.BaseType = 203))
) AS 'GrossThisYear',
(
(SELECT ISNULL(SUM(CASE T0.CANCELED WHEN 'C' THEN -(T0.DocTotal-T0.VatSum+T0.DpmAmnt-T0.TotalExpns-T0.RoundDif) ELSE (T0.DocTotal-T0.VatSum+T0.DpmAmnt-T0.TotalExpns-T0.RoundDif) END),0) FROM OINV T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @PreviousStart AND @PreviousEnd)
-
(SELECT ISNULL(SUM(CASE T0.CANCELED WHEN 'C' THEN -(T0.DocTotal-T0.VatSum+T0.DpmAmnt-T0.TotalExpns-T0.RoundDif) ELSE (T0.DocTotal-T0.VatSum+T0.DpmAmnt-T0.TotalExpns-T0.RoundDif) END),0) FROM ORIN T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @PreviousStart AND @PreviousEnd
AND NOT EXISTS (SELECT 1 FROM RIN1 TS1 WHERE TS1.DocEntry = T0.DocEntry AND TS1.BaseType = 203))
) AS 'SalesLastYear',
(
(SELECT ISNULL(SUM(CASE T0.CANCELED WHEN 'C' THEN -T0.GrosProfit ELSE T0.GrosProfit END),0) FROM OINV T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @PreviousStart AND @PreviousEnd)
-
(SELECT ISNULL(SUM(CASE T0.CANCELED WHEN 'C' THEN -T0.GrosProfit ELSE T0.GrosProfit END),0) FROM ORIN T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @PreviousStart AND @PreviousEnd
AND NOT EXISTS (SELECT 1 FROM RIN1 TS1 WHERE TS1.DocEntry = T0.DocEntry AND TS1.BaseType = 203))
) AS 'GrossLastYear',
(
(SELECT ISNULL(SUM(CASE T0.CANCELED WHEN 'C' THEN -(T0.DocTotal-T0.VatSum+T0.DpmAmnt-T0.TotalExpns-T0.RoundDif) ELSE (T0.DocTotal-T0.VatSum+T0.DpmAmnt-T0.TotalExpns-T0.RoundDif) END),0) FROM OINV T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @StartOfCurrentMonth AND @CurrentEnd)
-
(SELECT ISNULL(SUM(CASE T0.CANCELED WHEN 'C' THEN -(T0.DocTotal-T0.VatSum+T0.DpmAmnt-T0.TotalExpns-T0.RoundDif) ELSE (T0.DocTotal-T0.VatSum+T0.DpmAmnt-T0.TotalExpns-T0.RoundDif) END),0) FROM ORIN T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @StartOfCurrentMonth AND @CurrentEnd
AND NOT EXISTS (SELECT 1 FROM RIN1 TS1 WHERE TS1.DocEntry = T0.DocEntry AND TS1.BaseType = 203))
) AS 'SalesThisMonth',
(
(SELECT ISNULL(SUM(CASE T0.CANCELED WHEN 'C' THEN -T0.GrosProfit ELSE T0.GrosProfit END),0) FROM OINV T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @StartOfCurrentMonth AND @CurrentEnd)
-
(SELECT ISNULL(SUM(CASE T0.CANCELED WHEN 'C' THEN -T0.GrosProfit ELSE T0.GrosProfit END),0) FROM ORIN T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @StartOfCurrentMonth AND @CurrentEnd
AND NOT EXISTS (SELECT 1 FROM RIN1 TS1 WHERE TS1.DocEntry = T0.DocEntry AND TS1.BaseType = 203))
) AS 'GrossThisMonth',
(
(SELECT ISNULL(SUM(CASE T0.CANCELED WHEN 'C' THEN -(T0.DocTotal-T0.VatSum+T0.DpmAmnt-T0.TotalExpns-T0.RoundDif) ELSE (T0.DocTotal-T0.VatSum+T0.DpmAmnt-T0.TotalExpns-T0.RoundDif) END),0) FROM OINV T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @StartOfPreviousMonth AND @SameDayLastMonth)
-
(SELECT ISNULL(SUM(CASE T0.CANCELED WHEN 'C' THEN -(T0.DocTotal-T0.VatSum+T0.DpmAmnt-T0.TotalExpns-T0.RoundDif) ELSE (T0.DocTotal-T0.VatSum+T0.DpmAmnt-T0.TotalExpns-T0.RoundDif) END),0) FROM ORIN T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @StartOfPreviousMonth AND @SameDayLastMonth
AND NOT EXISTS (SELECT 1 FROM RIN1 TS1 WHERE TS1.DocEntry = T0.DocEntry AND TS1.BaseType = 203))
) AS 'SalesLastMonth',
(
(SELECT ISNULL(SUM(CASE T0.CANCELED WHEN 'C' THEN -T0.GrosProfit ELSE T0.GrosProfit END),0) FROM OINV T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @StartOfPreviousMonth AND @SameDayLastMonth)
-
(SELECT ISNULL(SUM(CASE T0.CANCELED WHEN 'C' THEN -T0.GrosProfit ELSE T0.GrosProfit END),0) FROM ORIN T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @StartOfPreviousMonth AND @SameDayLastMonth
AND NOT EXISTS (SELECT 1 FROM RIN1 TS1 WHERE TS1.DocEntry = T0.DocEntry AND TS1.BaseType = 203))
) AS 'GrossLastMonth',
(SELECT COUNT(*) FROM OINV T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @StartOfCurrentMonth AND @CurrentEnd) AS 'InvoicesThisMonth',
(SELECT COUNT(*) FROM OINV T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @StartOfPreviousMonth AND @SameDayLastMonth) AS 'InvoicesLastMonth',
(SELECT COUNT(*) FROM ORIN T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @StartOfCurrentMonth AND @CurrentEnd) AS 'CreditNotesThisMonth',
(SELECT COUNT(*) FROM ORIN T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @StartOfPreviousMonth AND @SameDayLastMonth) AS 'CreditNotesLastMonth',
(SELECT COUNT(*) FROM OINV T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @CurrentStart AND @CurrentEnd) AS 'InvoicesThisYear',
(SELECT COUNT(*) FROM OINV T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @PreviousStart AND @PreviousEnd) AS 'InvoicesLastYear',
(SELECT COUNT(*) FROM ORIN T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @CurrentStart AND @CurrentEnd) AS 'CreditNotesThisYear',
(SELECT COUNT(*) FROM ORIN T0 WITH (NOLOCK) WHERE T0.DocDate BETWEEN @PreviousStart AND @PreviousEnd) AS 'CreditNotesLastYear',
 @Today as 'Days'

 set @Period=@Period+1
END
--insert into DWH..temp Select * from @MTD
set @Y=@Y+1
END
select * from @MTD



