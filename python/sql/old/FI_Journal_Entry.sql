select T1.TransId, dbo.trans(T1.TransType) TransType, T1.RefDate, T1.Account,  T1.ContraAct,isnull(DebPayAcct,'') DebPayAcct,
REPLACE(REPLACE(REPLACE(T1.LineMemo, CHAR(9), ''), CHAR(10), ''), CHAR(13), '')  LineMemo, isnull(T1.Debit,0) Debit, isnull(T1.Credit,0) Credit, T2.AcctName, T1.U_InvNo,
isnull(T4.OcrName,'') as [KhoanMuc], isnull(T6.OcrName,'') as NhanHang, isnull(T8.OcrName,'') as NhomHang, isnull(T10.OcrName,'') AS DoiTac, isnull(T11.OcrName,'') AS NhanVien, isnull(T9.LocationName,'') LocationName,
T1.Ref3Line,REPLACE(REPLACE(REPLACE(T1.U_RemarksJE, CHAR(9), ''), CHAR(10), ''), CHAR(13), '')  Remarks,T1.Ref2,T5.U_S1No,T5.U_VoucherNo,T5.U_VoucherTypeID+' '+Name SoCT,T1.FCDebit,T1.FCCredit,isnull(T1.FCCurrency,'') FCCurrency,T12.U_NAME,T5.CreateDate SystemDate,T1.TaxDate,T1.U_AdjTran, 
isnull(T1.VatGroup,'') VatGroup, isnull(T1.BaseSum,0) BaseSum, isnull(T1.U_BPCode,'') U_BPCode, isnull(T1.U_BPName,'') U_BPName,  isnull(T1.U_TaxCode,'') U_TaxCode
, T5.BaseRef
from JDT1 T1 with(nolock) join OJDT T5 with(nolock) on T1.TransId = T5.TransId
join OACT T2 with(nolock)on T1.Account = T2.AcctCode
left join OOCR T4 with(nolock) on T1.ProfitCode = T4.OcrCode
left join OOCR T6 with(nolock) on T1.OcrCode2 = T6.OcrCode
left join OOCR T8 with(nolock) on T1.OcrCode3 = T8.OcrCode
left join OOCR T10 with(nolock) on T1.OcrCode4 = T10.OcrCode
left join OOCR T11 with(nolock) on T1.OcrCode5 = T11.OcrCode
left join (SELECT WhsCode, W.Location, L.Location AS LocationName FROM OWHS W LEFT JOIN OLCT L ON W.Location = L.Code) AS T9 ON T1.ProfitCode = T9.WhsCode
inner join OUSR T12 on T1.UserSign=t12.USERID
Left join [@VOUCHERTYPE] t13 on t13.Code=U_VoucherTypeID
--left join OINV T14 with(nolock) ON T14.U_S1No=T1.Ref3Line
--left join ORIN T15 with(nolock) ON T15.U_S1No=T5.U_S1No
left join ListBP T16 with(nolock) ON T16.CardCode=T1.ContraAct