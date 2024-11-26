DECLARE @Today DATETIME = CAST(CAST(GETDATE()  AS DATE) AS DATETIME)
SELECT 
case T0.CardType when 'S' then 'Vendor'  when 'C' then 'Customer'else 'N/A' end CardType,
T0.CardCode, T0.CardName,
T0.Balance,
(SELECT SUM(TS0.DocTotal-TS0.PaidSum) 
FROM OINV TS0 WITH (NOLOCK) 
WHERE TS0.DocStatus = 'O' AND TS0.DocDueDate < @Today AND TS0.CardCode = T0.CardCode) AS 'BalanceDue'
FROM OCRD T0
WHERE T0.Balance <> 0
AND T0.CardCode IN (SELECT TS0.CardCode FROM OINV TS0 WITH (NOLOCK) 
WHERE TS0.DocStatus = 'O' AND TS0.DocDueDate < @Today)
union
SELECT 
case T0.CardType when 'S' then 'Vendor'  when 'C' then 'Customer'else 'N/A' end CardType,
T0.CardCode,T0.CardName,
T0.Balance,
(SELECT SUM(TS0.DocTotal-TS0.PaidSum) 
FROM OPCH TS0 WITH (NOLOCK) 
WHERE TS0.DocStatus = 'O' AND TS0.DocDueDate < @Today AND TS0.CardCode = T0.CardCode) AS 'BalanceDue'
FROM OCRD T0
WHERE T0.Balance <> 0
AND T0.CardCode IN (SELECT TS0.CardCode FROM OPCH TS0 WITH (NOLOCK) 
WHERE TS0.DocStatus = 'O' AND TS0.DocDueDate < @Today)


