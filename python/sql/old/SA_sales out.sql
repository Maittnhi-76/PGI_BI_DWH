--Declare @FDate DateTime, @TDate DateTime, @CardCode Varchar (20), @ItmGrpN Varchar (100),  @ItemCode Varchar (100)
--Select @FDate = min(Q.Refdate) from OJDT Q where Q.Refdate >='[%0]'
--Select @TDate = max(R.Refdate) from OJDT R where R.Refdate <='[%1]'
--Select @CardCode = Min(S.CardCode) from OCRD S where S.CardCode like N'%[%3]%'
--Select @ItmGrpN = Min(T.ItmsGrpNam) from OITB T where T.ItmsGrpNam like N'%[%4]%'
--Select @ItemCode = Min(U.ItemCode) from OITM U where U.ItemCode like N'%[%5]%'
--Select @FDate, @TDate, @ItmGrpN, @ItemName

Select * from (
Select 'AR Invoice'[Type]
,case A.DocType When 'I' then 'Item' else 'Service' end[DocType]
,A.DocCur,A.DocDate,A.DocNum,A.U_S1No,A.U_VoucherNo,A.CardCode, C.CardName,D.GroupName[BP group],B.ItemCode
,Case A.DocType When 'I' then E.ItemName else B.Dscription end [ItemName/Description],B.U_isDiscount 'Loại hàng'
,B.Quantity[Quantity],B.GrossBuyPr 'Item Cost', U_OrigiPrice 'Giá chưa giảm',Round(U_OrigiDiscPrcnt,0) '% chiết khấu gốc',Round(U_DiscPrcnt,0) '% chiết khấu',B.Price 'Unit Price',VatPrcnt 'VAT',B.PriceAfVAT
,B.Rate, B.Currency [Price Currency]
,B.LineTotal- (B.LineTotal*isnull(A.DiscPrcnt,0)/100)[LineTotalLC],B.GrssProfit
,B.TotalFrgn- (B.TotalFrgn*isnull(A.DiscPrcnt,0)/100)[LineTotalFC],B.GTotalFC,CogsOcrCod 'Khoản mục',OcrCode2 'Nhãn hàng',OcrCode3 'Nhóm hàng',OcrCode4 'Đối tác',OcrCode5 'Nhân viên'
,Case When B.SlpCode<>'-1' then F.SlpName
	When B.SlpCode='-1' and A.SlpCode<>'-1' then (Select M.SlpName from OSLP M where A.SlpCode = M.SlpCode) else '' end [Sales Person]
--Select B.*,
from OINV A
  Left Outer Join INV1 B on A.Docentry = B.DocEntry
  Left Outer Join OCRD C on A.CardCode = C.CardCode
  Left Outer Join OCRG D on C.GroupCode = D.GroupCode
  left outer join OITM E on B.ItemCode = E.ItemCode
  Left Outer Join OSLP F on B.SlpCode = F.SlpCode
  Left Outer Join OITB G on E.ItmsGrpCod = G.ItmsGrpCod
Where A.Canceled = 'N' 
--and A.DocEntry=191509
--  and A.DocDate >=@FDate and A.DocDate  <=@TDate
--  and D.GroupName Like N'%[%2]%'
--  and C.CardCode  Like '%[%3]%'
--  and (G.ItmsGrpNam Like N'%[%4]%' or G.ItmsGrpNam is null)
--  and (B.ItemCode Like '%[%5]%' or B.ItemCode is null)
Union All
Select 'AR Credit Note'[Type]
,case A.DocType When 'I' then 'Item' else 'Service' end [DocType]
,A.DocCur,A.DocDate, A.DocNum,A.U_S1No,A.U_VoucherNo,A.CardCode, C.CardName,D.GroupName[BP group],B.ItemCode
,Case A.DocType When 'I' then E.ItemName else B.Dscription end [ItemName/Description],B.U_isDiscount 'Loại hàng'
,Case when B.NoInvtryMv ='Y' then 0 else -B.Quantity end [Quantity]
--,B.Quantity[Quantity]
,B.GrossBuyPr 'Item Cost', U_OrigiPrice 'Giá chưa giảm',Round(U_OrigiDiscPrcnt,0) '% chiết khấu gốc',Round(U_DiscPrcnt,0) '% chiết khấu',B.Price 'Unit Price',VatPrcnt 'VAT',B.PriceAfVAT
,B.Rate, B.Currency [Price Currency],-B.LineTotal+ (B.LineTotal*isnull(A.DiscPrcnt,0)/100)[LineTotal],-B.GrssProfit
,-B.TotalFrgn+ (B.TotalFrgn*isnull(A.DiscPrcnt,0)/100)[LineTotalFC],-B.GTotalFC,CogsOcrCod 'Khoản mục',OcrCode2 'Nhãn hàng',OcrCode3 'Nhóm hàng',OcrCode4 'Đối tác',OcrCode5 'Nhân viên'
,Case When B.SlpCode<>'-1' then F.SlpName When B.SlpCode='-1' and A.SlpCode<>'-1' then (Select M.SlpName from OSLP M where A.SlpCode = M.SlpCode) else '' end [Sales Person]
from ORIN A
  Left Outer Join RIN1 B on A.Docentry = B.DocEntry
  Left Outer Join OCRD C on A.CardCode = C.CardCode
  Left Outer Join OCRG D on C.GroupCode = D.GroupCode
  left outer join OITM E on B.ItemCode = E.ItemCode
  Left Outer Join OSLP F on B.SlpCode = F.SlpCode
  Left Outer Join OITB G on E.ItmsGrpCod = G.ItmsGrpCod
Where A.Canceled = 'N'
  --and A.DocDate >=@FDate and A.DocDate  <=@TDate
  --and D.GroupName Like N'%[%2]%'
  --and C.CardCode  Like '%[%3]%'
  --and (G.ItmsGrpNam Like N'%[%4]%' or G.ItmsGrpNam is null)
  --and (B.ItemCode Like '%[%5]%' or B.ItemCode is null)
union
Select 'Sales Order Pending Kho'[Type]
,case A.DocType When 'I' then 'Item' else 'Service' end [DocType]
,A.DocCur,A.DocDate, A.DocNum,A.U_S1No,A.U_VoucherNo,A.CardCode, C.CardName,D.GroupName[BP group],B.ItemCode
,Case A.DocType When 'I' then E.ItemName else B.Dscription end [ItemName/Description],B.U_isDiscount 'Loại hàng'
,B.Quantity[Quantity],B.GrossBuyPr 'Item Cost', U_OrigiPrice 'Giá chưa giảm',Round(U_OrigiDiscPrcnt,0) '% chiết khấu gốc',Round(U_DiscPrcnt,0) '% chiết khấu',B.Price 'Unit Price',VatPrcnt 'VAT',B.PriceAfVAT
,B.Rate, B.Currency [Price Currency]
,B.LineTotal- (B.LineTotal*isnull(A.DiscPrcnt,0)/100)[LineTotalLC],B.GrssProfit
,B.TotalFrgn- (B.TotalFrgn*isnull(A.DiscPrcnt,0)/100)[LineTotalFC],B.GTotalFC,CogsOcrCod 'Khoản mục',OcrCode2 'Nhãn hàng',OcrCode3 'Nhóm hàng',OcrCode4 'Đối tác',OcrCode5 'Nhân viên'
,Case When B.SlpCode<>'-1' then F.SlpName
	When B.SlpCode='-1' and A.SlpCode<>'-1' then (Select M.SlpName from OSLP M where A.SlpCode = M.SlpCode) else '' end [Sales Person]
--Select B.*
from ORDR A
  Left Outer Join RDR1 B on A.Docentry = B.DocEntry
  Left Outer Join OCRD C on A.CardCode = C.CardCode
  Left Outer Join OCRG D on C.GroupCode = D.GroupCode
  left outer join OITM E on B.ItemCode = E.ItemCode
  Left Outer Join OSLP F on B.SlpCode = F.SlpCode
  Left Outer Join OITB G on E.ItmsGrpCod = G.ItmsGrpCod
Where DocStatus ='O'

Union

Select 'Sales Order Pending KT'[Type]
,case A.DocType When 'I' then 'Item' else 'Service' end [DocType]
,A.DocCur,A.DocDate, A.DocNum,A.U_S1No,A.U_VoucherNo,A.CardCode, C.CardName,D.GroupName[BP group],B.ItemCode
,Case A.DocType When 'I' then E.ItemName else B.Dscription end [ItemName/Description],B.U_isDiscount 'Loại hàng'
,B.Quantity[Quantity],B.GrossBuyPr 'Item Cost', U_OrigiPrice 'Giá chưa giảm',Round(U_OrigiDiscPrcnt,0) '% chiết khấu gốc',Round(U_DiscPrcnt,0) '% chiết khấu',B.Price 'Unit Price',VatPrcnt 'VAT',B.PriceAfVAT
,B.Rate, B.Currency [Price Currency]
,B.LineTotal- (B.LineTotal*isnull(A.DiscPrcnt,0)/100)[LineTotalLC],B.GrssProfit
,B.TotalFrgn- (B.TotalFrgn*isnull(A.DiscPrcnt,0)/100)[LineTotalFC],B.GTotalFC,CogsOcrCod 'Khoản mục',OcrCode2 'Nhãn hàng',OcrCode3 'Nhóm hàng',OcrCode4 'Đối tác',OcrCode5 'Nhân viên'
,Case When B.SlpCode<>'-1' then F.SlpName
	When B.SlpCode='-1' and A.SlpCode<>'-1' then (Select M.SlpName from OSLP M where A.SlpCode = M.SlpCode) else '' end [Sales Person]
--Select B.*
from ODRF A
  Left Outer Join DRF1 B on A.Docentry = B.DocEntry
  Left Outer Join OCRD C on A.CardCode = C.CardCode
  Left Outer Join OCRG D on C.GroupCode = D.GroupCode
  left outer join OITM E on B.ItemCode = E.ItemCode
  Left Outer Join OSLP F on B.SlpCode = F.SlpCode
  Left Outer Join OITB G on E.ItmsGrpCod = G.ItmsGrpCod
Where DocStatus ='O'

--Order BY 3
) t
where t.DocDate>='2024-09-01'


