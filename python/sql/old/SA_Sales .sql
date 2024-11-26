SELECT T0.[DocNum],T0.[DocDate],Case when T0.[ObjType] = 13 then 'INVOICE' when T0.[ObjType] = 14 then 'CREDIT NOTE' end as 'Type',
T0.[CardCode],T0.[CardName],T1.[ItemCode],T1.[Dscription],T3.[ItmsGrpNam],T4.[FirmName],T8.[ItmsGrpNam],T5.[SlpName],T7.[State2],T1.[Quantity],T0.[DocCur],T1.[LineTotal] as 'Canadian$',
Case when T1.[Rate] = 0 then T1.[LineTotal] else (T1.[LineTotal]/T1.[Rate]) end as 'Equivalent Value',
Case when (T1.[Quantity] = 0 and (T1.[StockPrice]*T1.[Quantity])= 0 ) then T1.[LineTotal] else (T1.[INMPrice]*T1.[Quantity]) end as 'Sales - Net',(T1.[StockPrice]*T1.[Quantity]) as 'Cost',
Case when T0.[DiscPrcnt] <> '0' then T0.[DiscPrcnt] Else T1.[DiscPrcnt] end as 'Discount %age',T0.[TotalExpns] as 'Freight',T1.[GrssProfit],
Case when (T1.[INMPrice]*T1.[Quantity]) =0 then 0 else (T1.[GrssProfit]/(T1.[INMPrice]*T1.[Quantity]))*100 end as 'GP %age'
FROM OINV T0 
INNER JOIN INV1 T1 ON T0.[DocEntry] = T1.[DocEntry] 
INNER JOIN OITM T2 ON T1.[ItemCode] = T2.[ItemCode] 
INNER JOIN OITB T3 ON T2.[ItmsGrpCod] = T3.[ItmsGrpCod] 
INNER JOIN OMRC T4 ON T2.[FirmCode] = T4.[FirmCode] 
INNER JOIN OSLP T5 ON T0.[SlpCode] = T5.[SlpCode] 
INNER JOIN INV12 T6 ON T0.[DocEntry] = T6.[DocEntry] 
INNER JOIN OCRD T7 ON T0.[CardCode] = T7.[CardCode] 
LEFT JOIN OITG T8 ON CASE 
WHEN ISNULL(T2.QryGroup1, '') = 'Y' THEN 1 
WHEN ISNULL(T2.QryGroup2, '') = 'Y' THEN 2 
WHEN ISNULL(T2.QryGroup3, '') = 'Y' THEN 3 
WHEN ISNULL(T2.QryGroup4, '') = 'Y' THEN 4
WHEN ISNULL(T2.QryGroup5, '') = 'Y' THEN 5 
WHEN ISNULL(T2.QryGroup6, '') = 'Y' THEN 6
WHEN ISNULL(T2.QryGroup7, '') = 'Y' THEN 7 
WHEN ISNULL(T2.QryGroup8, '') = 'Y' THEN 8
WHEN ISNULL(T2.QryGroup9, '') = 'Y' THEN 9 
WHEN ISNULL(T2.QryGroup10, '') = 'Y' THEN 10
WHEN ISNULL(T2.QryGroup11, '') = 'Y' THEN 11 
WHEN ISNULL(T2.QryGroup12, '') = 'Y' THEN 12
WHEN ISNULL(T2.QryGroup13, '') = 'Y' THEN 13 
WHEN ISNULL(T2.QryGroup14, '') = 'Y' THEN 14
WHEN ISNULL(T2.QryGroup15, '') = 'Y' THEN 15 
WHEN ISNULL(T2.QryGroup16, '') = 'Y' THEN 16
WHEN ISNULL(T2.QryGroup17, '') = 'Y' THEN 17 
WHEN ISNULL(T2.QryGroup18, '') = 'Y' THEN 18
WHEN ISNULL(T2.QryGroup19, '') = 'Y' THEN 19 
WHEN ISNULL(T2.QryGroup20, '') = 'Y' THEN 20
WHEN ISNULL(T2.QryGroup21, '') = 'Y' THEN 21 
WHEN ISNULL(T2.QryGroup22, '') = 'Y' THEN 22
WHEN ISNULL(T2.QryGroup23, '') = 'Y' THEN 23 
WHEN ISNULL(T2.QryGroup24, '') = 'Y' THEN 24
WHEN ISNULL(T2.QryGroup25, '') = 'Y' THEN 25 
WHEN ISNULL(T2.QryGroup26, '') = 'Y' THEN 26
WHEN ISNULL(T2.QryGroup27, '') = 'Y' THEN 27 
WHEN ISNULL(T2.QryGroup28, '') = 'Y' THEN 28
WHEN ISNULL(T2.QryGroup29, '') = 'Y' THEN 29 
WHEN ISNULL(T2.QryGroup30, '') = 'Y' THEN 30
WHEN ISNULL(T2.QryGroup31, '') = 'Y' THEN 31 
WHEN ISNULL(T2.QryGroup32, '') = 'Y' THEN 32
WHEN ISNULL(T2.QryGroup33, '') = 'Y' THEN 33 
WHEN ISNULL(T2.QryGroup34, '') = 'Y' THEN 34
WHEN ISNULL(T2.QryGroup35, '') = 'Y' THEN 35 
WHEN ISNULL(T2.QryGroup36, '') = 'Y' THEN 36
WHEN ISNULL(T2.QryGroup37, '') = 'Y' THEN 37 
WHEN ISNULL(T2.QryGroup38, '') = 'Y' THEN 38
WHEN ISNULL(T2.QryGroup39, '') = 'Y' THEN 39 
WHEN ISNULL(T2.QryGroup40, '') = 'Y' THEN 40
WHEN ISNULL(T2.QryGroup41, '') = 'Y' THEN 41 
WHEN ISNULL(T2.QryGroup42, '') = 'Y' THEN 42
WHEN ISNULL(T2.QryGroup43, '') = 'Y' THEN 43 
WHEN ISNULL(T2.QryGroup44, '') = 'Y' THEN 44
WHEN ISNULL(T2.QryGroup45, '') = 'Y' THEN 45 
WHEN ISNULL(T2.QryGroup46, '') = 'Y' THEN 46
WHEN ISNULL(T2.QryGroup47, '') = 'Y' THEN 47 
WHEN ISNULL(T2.QryGroup48, '') = 'Y' THEN 48
WHEN ISNULL(T2.QryGroup49, '') = 'Y' THEN 49 
WHEN ISNULL(T2.QryGroup50, '') = 'Y' THEN 50
WHEN ISNULL(T2.QryGroup51, '') = 'Y' THEN 51
 WHEN ISNULL(T2.QryGroup52, '') = 'Y' THEN 52WHEN ISNULL(T2.QryGroup53, '') = 'Y' THEN 53 WHEN ISNULL(T2.QryGroup54, '') = 'Y' THEN 54
 WHEN ISNULL(T2.QryGroup55, '') = 'Y' THEN 55 WHEN ISNULL(T2.QryGroup56, '') = 'Y' THEN 56WHEN ISNULL(T2.QryGroup57, '') = 'Y' THEN 57 WHEN ISNULL(T2.QryGroup58, '') = 'Y' THEN 58
 WHEN ISNULL(T2.QryGroup59, '') = 'Y' THEN 59 WHEN ISNULL(T2.QryGroup60, '') = 'Y' THEN 60WHEN ISNULL(T2.QryGroup61, '') = 'Y' THEN 61 WHEN ISNULL(T2.QryGroup62, '') = 'Y' THEN 62
 WHEN ISNULL(T2.QryGroup63, '') = 'Y' THEN 63 WHEN ISNULL(T2.QryGroup64, '') = 'Y' THEN 64 END = ItmsTypCod 
 WHERE T0.[CANCELED] = 'N' 
 And T0.[DocDate] >= '2024-10-01'
 Union All
Select T0.[DocNum],T0.[DocDate], Case when T0.[ObjType] = 13 then 'INVOICE' when T0.[ObjType] = 14 then 'CREDIT NOTE' end as 'Type',
 T0.[CardCode],T0.[CardName],T1.[ItemCode],T1.[Dscription],T3.[ItmsGrpNam],T4.[FirmName],T8.[ItmsGrpNam],T5.[SlpName],T7.[State2],T1.[Quantity]*-1,T0.[DocCur],T1.[LineTotal]*-1 as 'Canadian$',
 Case when T1.[Rate] = 0 then T1.[LineTotal]*-1 else (T1.[LineTotal]/T1.[Rate])*-1 end as 'Equivalent Value',
 Case when (T1.[Quantity] = 0 and (T1.[StockPrice]*T1.[Quantity])= 0 ) then T1.[LineTotal]*-1 else (T1.[INMPrice]*T1.[Quantity])*-1 end as 'Sales - Net',(T1.[StockPrice]*T1.[Quantity])*-1 as 'Cost',
 Case when T0.[DiscPrcnt] <> '0' then T0.[DiscPrcnt] Else T1.[DiscPrcnt] end as 'Discount %age',T0.[TotalExpns] as 'Freight',T1.[GrssProfit]*-1, 
 Case when (T1.[INMPrice]*T1.[Quantity]) =0 then 0 else (T1.[GrssProfit]/(T1.[INMPrice]*T1.[Quantity]))*100*-1 end as 'GP %age'
 FROM ORIN T0 
 INNER JOIN RIN1 T1 ON T0.[DocEntry] = T1.[DocEntry] 
 INNER JOIN OITM T2 ON T1.[ItemCode] = T2.[ItemCode] 
 INNER JOIN OITB T3 ON T2.[ItmsGrpCod] = T3.[ItmsGrpCod] 
 INNER JOIN OMRC T4 ON T2.[FirmCode] = T4.[FirmCode] 
 INNER JOIN OSLP T5 ON T0.[SlpCode] = T5.[SlpCode] 
 INNER JOIN RIN12 T6 ON T0.[DocEntry] = T6.[DocEntry] 
 INNER JOIN OCRD T7 ON T0.[CardCode] = T7.[CardCode] 
 LEFT JOIN OITG T8 ON CASE 
 WHEN ISNULL(T2.QryGroup1, '') = 'Y' THEN 1 
 WHEN ISNULL(T2.QryGroup2, '') = 'Y' THEN 2WHEN ISNULL(T2.QryGroup3, '') = 'Y' THEN 3 WHEN ISNULL(T2.QryGroup4, '') = 'Y' THEN 4WHEN ISNULL(T2.QryGroup5, '') = 'Y' THEN 5 
 WHEN ISNULL(T2.QryGroup6, '') = 'Y' THEN 6WHEN ISNULL(T2.QryGroup7, '') = 'Y' THEN 7 WHEN ISNULL(T2.QryGroup8, '') = 'Y' THEN 8WHEN ISNULL(T2.QryGroup9, '') = 'Y' THEN 9 
 WHEN ISNULL(T2.QryGroup10, '') = 'Y' THEN 10WHEN ISNULL(T2.QryGroup11, '') = 'Y' THEN 11 WHEN ISNULL(T2.QryGroup12, '') = 'Y' THEN 12WHEN ISNULL(T2.QryGroup13, '') = 'Y' THEN 13 
 WHEN ISNULL(T2.QryGroup14, '') = 'Y' THEN 14WHEN ISNULL(T2.QryGroup15, '') = 'Y' THEN 15 WHEN ISNULL(T2.QryGroup16, '') = 'Y' THEN 16WHEN ISNULL(T2.QryGroup17, '') = 'Y' THEN 17 
 WHEN ISNULL(T2.QryGroup18, '') = 'Y' THEN 18WHEN ISNULL(T2.QryGroup19, '') = 'Y' THEN 19 WHEN ISNULL(T2.QryGroup20, '') = 'Y' THEN 20WHEN ISNULL(T2.QryGroup21, '') = 'Y' THEN 21 
 WHEN ISNULL(T2.QryGroup22, '') = 'Y' THEN 22WHEN ISNULL(T2.QryGroup23, '') = 'Y' THEN 23 WHEN ISNULL(T2.QryGroup24, '') = 'Y' THEN 24WHEN ISNULL(T2.QryGroup25, '') = 'Y' THEN 25 
 WHEN ISNULL(T2.QryGroup26, '') = 'Y' THEN 26WHEN ISNULL(T2.QryGroup27, '') = 'Y' THEN 27 WHEN ISNULL(T2.QryGroup28, '') = 'Y' THEN 28WHEN ISNULL(T2.QryGroup29, '') = 'Y' THEN 29 
 WHEN ISNULL(T2.QryGroup30, '') = 'Y' THEN 30WHEN ISNULL(T2.QryGroup31, '') = 'Y' THEN 31 WHEN ISNULL(T2.QryGroup32, '') = 'Y' THEN 32WHEN ISNULL(T2.QryGroup33, '') = 'Y' THEN 33 
 WHEN ISNULL(T2.QryGroup34, '') = 'Y' THEN 34WHEN ISNULL(T2.QryGroup35, '') = 'Y' THEN 35 WHEN ISNULL(T2.QryGroup36, '') = 'Y' THEN 36WHEN ISNULL(T2.QryGroup37, '') = 'Y' THEN 37 
 WHEN ISNULL(T2.QryGroup38, '') = 'Y' THEN 38WHEN ISNULL(T2.QryGroup39, '') = 'Y' THEN 39 WHEN ISNULL(T2.QryGroup40, '') = 'Y' THEN 40WHEN ISNULL(T2.QryGroup41, '') = 'Y' THEN 41 
 WHEN ISNULL(T2.QryGroup42, '') = 'Y' THEN 42WHEN ISNULL(T2.QryGroup43, '') = 'Y' THEN 43 WHEN ISNULL(T2.QryGroup44, '') = 'Y' THEN 44WHEN ISNULL(T2.QryGroup45, '') = 'Y' THEN 45 
 WHEN ISNULL(T2.QryGroup46, '') = 'Y' THEN 46WHEN ISNULL(T2.QryGroup47, '') = 'Y' THEN 47 WHEN ISNULL(T2.QryGroup48, '') = 'Y' THEN 48WHEN ISNULL(T2.QryGroup49, '') = 'Y' THEN 49 
 WHEN ISNULL(T2.QryGroup50, '') = 'Y' THEN 50WHEN ISNULL(T2.QryGroup51, '') = 'Y' THEN 51 WHEN ISNULL(T2.QryGroup52, '') = 'Y' THEN 52WHEN ISNULL(T2.QryGroup53, '') = 'Y' THEN 53 
 WHEN ISNULL(T2.QryGroup54, '') = 'Y' THEN 54WHEN ISNULL(T2.QryGroup55, '') = 'Y' THEN 55 WHEN ISNULL(T2.QryGroup56, '') = 'Y' THEN 56WHEN ISNULL(T2.QryGroup57, '') = 'Y' THEN 57 
 WHEN ISNULL(T2.QryGroup58, '') = 'Y' THEN 58WHEN ISNULL(T2.QryGroup59, '') = 'Y' THEN 59 WHEN ISNULL(T2.QryGroup60, '') = 'Y' THEN 60WHEN ISNULL(T2.QryGroup61, '') = 'Y' THEN 61 
 WHEN ISNULL(T2.QryGroup62, '') = 'Y' THEN 62WHEN ISNULL(T2.QryGroup63, '') = 'Y' THEN 63 WHEN ISNULL(T2.QryGroup64, '') = 'Y' THEN 64 END = ItmsTypCod 
 WHERE T0.[CANCELED] = 'N' 
 and T0.[DocDate] >= '2024-10-01'