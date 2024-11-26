SELECT DISTINCT U_InvNo 'Số h.đơn',U_Date13 'Ngày đến kho',U_ContractNo 'Số hợp đồng',U_IDCCode 'Số tờ khai',U_BL 'Bill of Lading',
T0.DocEntry,t0.DocDate,
T2.DOCENTRY [Landed Cost 1 Number],T5.DOCENTRY [Landed Cost 2 Number],t5.TargetDoc [Landed Cost 3 Number],t8.TargetDoc [Landed Cost 4 Number],t11.TargetDoc [Landed Cost 5 Number],
T1.[ItemCode],T1.[Dscription][Desription],T1.[Quantity],T1.[Price],T1.[LineTotal],
Case when t11.TargetDoc is not null then T16.AlcCode
when t11.TargetDoc is null and  t8.TargetDoc is not null then T13.AlcCode
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is not null  then T10.AlcCode 
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is null and T5.DOCENTRY is not null  then T7.AlcCode
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is null and T5.DOCENTRY is null and T2.DOCENTRY is not null  then T4.AlcCode
end AlcCode,
Case when t11.TargetDoc is not null then T16.AlcName
when t11.TargetDoc is null and  t8.TargetDoc is not null then T13.AlcName
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is not null  then T10.AlcName 
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is null and T5.DOCENTRY is not null  then  T7.AlcName
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is null and T5.DOCENTRY is null and T2.DOCENTRY is not null  then T4.AlcName
end AlcName,
Case when t11.TargetDoc is not null then T15.costsum
when t11.TargetDoc is null and  t8.TargetDoc is not null then T12.costsum
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is not null  then  T9.costsum 
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is null and T5.DOCENTRY is not null  then  T6.costsum
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is null and T5.DOCENTRY is null and T2.DOCENTRY is not null then T3.costsum
end [Landed Cost Cost Total],
Case when t11.TargetDoc is not null then (T14.FoBValue/(T0.DocTotal-T0.[VatSum]))*t15.costsum
when t11.TargetDoc is null and  t8.TargetDoc is not null then (T11.FoBValue/(T0.DocTotal-T0.[VatSum]))*t12.costsum
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is not null  then (T8.FoBValue/(T0.DocTotal-T0.[VatSum]))*t9.costsum
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is null and T5.DOCENTRY is not null  then (T5.FoBValue/(T0.DocTotal-T0.[VatSum]))*t6.costsum
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is null and T5.DOCENTRY is null and T2.DOCENTRY is not null  then (T2.FoBValue/(T0.DocTotal-T0.[VatSum]))*t3.costsum
end [Freight Per SKU], 2.DOCENTRY 
--,T4.AlcCode ,T4.AlcName [Landed Cost 2 Cost Type],t3.costsum [Landed Cost Cost Total],(T2.FoBValue/(T0.DocTotal-T0.[VatSum]))*t3.costsum[Freight Per SKU]
--T7.AlcCode ,T7.AlcName [Landed Cost 2 Cost Type],t6.costsum [Landed Cost Cost Total],(T5.FoBValue/(T0.DocTotal-T0.[VatSum]))*t6.costsum[Freight Per SKU],
--T10.AlcCode ,T10.AlcName [Landed Cost 2 Cost Type],t9.costsum [Landed Cost Cost Total],(T8.FoBValue/(T0.DocTotal-T0.[VatSum]))*t9.costsum[Freight Per SKU],
--T13.AlcCode ,T13.AlcName [Landed Cost 2 Cost Type],t12.costsum [Landed Cost Cost Total],(T11.FoBValue/(T0.DocTotal-T0.[VatSum]))*t12.costsum[Freight Per SKU]
--select *
FROM OPDN T0 
inner JOIN PDN1 T1 ON T0.DocEntry = T1.DocEntry 
inner join IPF1 T2 on T2.[BaseType] = 20 and T2.[BaseEntry] = T1.[DocEntry] and T2.[ItemCode] = T1.[ItemCode]
inner join IPF2 T3 on t2.docentry = t3.docentry 
inner JOIN OALC T4 ON T3.AlcCode = T4.AlcCode 
left outer join IPF1 T5 on T5.[BaseType] = 69 and T5.[BaseEntry] = T2.[DocEntry] and T5.[ItemCode] = T2.[ItemCode]
left outer JOIN IPF2 T6 ON T5.docentry = T6.docentry
left outer JOIN OALC T7 ON T6.AlcCode = T7.AlcCode

left outer join IPF1 T8 on T8.[BaseType] = 69 and T8.[BaseEntry] = T5.[DocEntry] and T8.[ItemCode] = T5.[ItemCode]
left outer JOIN IPF2 T9 ON T8.docentry = T9.docentry
left outer JOIN OALC T10 ON T9.AlcCode = T10.AlcCode

left outer join IPF1 T11 on T11.[BaseType] = 69 and T11.[BaseEntry] = T8.[DocEntry] and T11.[ItemCode] = T8.[ItemCode]
left outer JOIN IPF2 T12 ON T11.docentry = T12.docentry
left outer JOIN OALC T13 ON T12.AlcCode = T13.AlcCode

left outer join IPF1 T14 on T14.[BaseType] = 69 and T14.[BaseEntry] = T11.[DocEntry] and T14.[ItemCode] = T11.[ItemCode]
left outer JOIN IPF2 T15 ON T14.docentry = T15.docentry
left outer JOIN OALC T16 ON T15.AlcCode = T16.AlcCode

where t0.DocDate between '2014-01-01' and '2016-09-30'
 --or T2.DOCENTRY=615
--or T5.DOCENTRY=616 or t5.TargetDoc=617 or t8.TargetDoc=618 or t11.TargetDoc=633
--where T5.DOCENTRY ='379'
--group by T7.AlcCode ,T0.[DocNum],T0.[DocDate],T1.[ItemCode],T1.[Dscription],T1.[Quantity],T1.[Price],T1.[LineTotal],T2.[DocEntry],T4.[AlcName],T3.[CostSum],T5.DOCENTRY,T7.AlcName,t6.costsum,T5.FoBValue,T0.DocTotal,T0.[VatSum]

---1)
--0
--20) Goods Receipt PO
--69) Landed Costs
--18) A/P Invoice
union
SELECT DISTINCT U_InvNo 'Số h.đơn',U_Date13 'Ngày đến kho',U_ContractNo 'Số hợp đồng',U_IDCCode 'Số tờ khai',U_BL 'Bill of Lading',
T0.DocEntry,t0.DocDate,
T2.DOCENTRY [Landed Cost 1 Number],T5.DOCENTRY [Landed Cost 2 Number],t5.TargetDoc [Landed Cost 3 Number],t8.TargetDoc [Landed Cost 4 Number],t11.TargetDoc [Landed Cost 5 Number],
T1.[ItemCode],T1.[Dscription][Desription],T1.[Quantity],T1.[Price],T1.[LineTotal],
Case when t11.TargetDoc is not null then T16.AlcCode
when t11.TargetDoc is null and  t8.TargetDoc is not null then T13.AlcCode
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is not null  then T10.AlcCode 
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is null and T5.DOCENTRY is not null  then T7.AlcCode
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is null and T5.DOCENTRY is null and T2.DOCENTRY is not null  then T4.AlcCode
end AlcCode,
Case when t11.TargetDoc is not null then T16.AlcName
when t11.TargetDoc is null and  t8.TargetDoc is not null then T13.AlcName
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is not null  then T10.AlcName 
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is null and T5.DOCENTRY is not null  then  T7.AlcName
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is null and T5.DOCENTRY is null and T2.DOCENTRY is not null  then T4.AlcName
end AlcName,
Case when t11.TargetDoc is not null then T15.costsum
when t11.TargetDoc is null and  t8.TargetDoc is not null then T12.costsum
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is not null  then  T9.costsum 
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is null and T5.DOCENTRY is not null  then  T6.costsum
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is null and T5.DOCENTRY is null and T2.DOCENTRY is not null then T3.costsum
end [Landed Cost Cost Total],
Case when t11.TargetDoc is not null then (T14.FoBValue/(T0.DocTotal-T0.[VatSum]))*t15.costsum
when t11.TargetDoc is null and  t8.TargetDoc is not null then (T11.FoBValue/(T0.DocTotal-T0.[VatSum]))*t12.costsum
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is not null  then (T8.FoBValue/(T0.DocTotal-T0.[VatSum]))*t9.costsum
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is null and T5.DOCENTRY is not null  then (T5.FoBValue/(T0.DocTotal-T0.[VatSum]))*t6.costsum
when t11.TargetDoc is null and  t8.TargetDoc is null and t5.TargetDoc is null and T5.DOCENTRY is null and T2.DOCENTRY is not null  then (T2.FoBValue/(T0.DocTotal-T0.[VatSum]))*t3.costsum
end [Freight Per SKU], 2.DOCENTRY 
--,T4.AlcCode ,T4.AlcName [Landed Cost 2 Cost Type],t3.costsum [Landed Cost Cost Total],(T2.FoBValue/(T0.DocTotal-T0.[VatSum]))*t3.costsum[Freight Per SKU]
--T7.AlcCode ,T7.AlcName [Landed Cost 2 Cost Type],t6.costsum [Landed Cost Cost Total],(T5.FoBValue/(T0.DocTotal-T0.[VatSum]))*t6.costsum[Freight Per SKU],
--T10.AlcCode ,T10.AlcName [Landed Cost 2 Cost Type],t9.costsum [Landed Cost Cost Total],(T8.FoBValue/(T0.DocTotal-T0.[VatSum]))*t9.costsum[Freight Per SKU],
--T13.AlcCode ,T13.AlcName [Landed Cost 2 Cost Type],t12.costsum [Landed Cost Cost Total],(T11.FoBValue/(T0.DocTotal-T0.[VatSum]))*t12.costsum[Freight Per SKU]
--select *
FROM OPCH T0 
inner JOIN PCH1 T1 ON T0.DocEntry = T1.DocEntry 
inner join IPF1 T2 on T2.[BaseType] = 18 and T2.[BaseEntry] = T1.[DocEntry] and T2.[ItemCode] = T1.[ItemCode]
inner join IPF2 T3 on t2.docentry = t3.docentry 
inner JOIN OALC T4 ON T3.AlcCode = T4.AlcCode 
left outer join IPF1 T5 on T5.[BaseType] = 69 and T5.[BaseEntry] = T2.[DocEntry] and T5.[ItemCode] = T2.[ItemCode]
left outer JOIN IPF2 T6 ON T5.docentry = T6.docentry
left outer JOIN OALC T7 ON T6.AlcCode = T7.AlcCode

left outer join IPF1 T8 on T8.[BaseType] = 69 and T8.[BaseEntry] = T5.[DocEntry] and T8.[ItemCode] = T5.[ItemCode]
left outer JOIN IPF2 T9 ON T8.docentry = T9.docentry
left outer JOIN OALC T10 ON T9.AlcCode = T10.AlcCode

left outer join IPF1 T11 on T11.[BaseType] = 69 and T11.[BaseEntry] = T8.[DocEntry] and T11.[ItemCode] = T8.[ItemCode]
left outer JOIN IPF2 T12 ON T11.docentry = T12.docentry
left outer JOIN OALC T13 ON T12.AlcCode = T13.AlcCode

left outer join IPF1 T14 on T14.[BaseType] = 69 and T14.[BaseEntry] = T11.[DocEntry] and T14.[ItemCode] = T11.[ItemCode]
left outer JOIN IPF2 T15 ON T14.docentry = T15.docentry
left outer JOIN OALC T16 ON T15.AlcCode = T16.AlcCode
where t0.DocDate between '2014-01-01' and '2016-09-30'