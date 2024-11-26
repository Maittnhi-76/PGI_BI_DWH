SELECT T1.Account, 
	(SELECT SUM(isnull(S0.Debit,0) - isnull(S0.Credit,0)) FROM JDT1 S0 
	WHERE S0.Account like '6%%' and S0.RefDate<'20130301') as OpeningBalance
	, SUM(isnull(T1.Debit,0)) as Debit
	, SUM(isnull(T1.Credit,0)) as Credit, 
	(SELECT SUM(isnull(S0.Debit,0) - isnull(S0.Credit,0)) FROM JDT1 S0 
	WHERE S0.Account like '6%%' and S0.RefDate<='2024-09-30') as ClosingBalance
FROM JDT1 T1
WHERE T1.Account like '6%%' and T1.RefDate>='20130501' and T1.RefDate<='2024-09-30'
GROUP BY T1.Accountgi