With 
S_ORDR AS (
  SELECT DocEntry	,DocNum	,DocType	,CANCELED	,DocStatus	,DocDueDate	,DocTotal	,GrosProfit	,JrnlMemo	,DocTotalSy	,PaidSys	,GrosProfSy	,UpdateDate	,CreateDate	,CreateTs,TaxDate	,ReqDate	,ExtraDays	,SlpCode	,U_S1No   	,U_CardCode	,U_CardName	,U_NoteForAll,U_NoteForAcc	,U_NoteForWhs	,U_NoteForStatus	,U_Store	,U_TotalQty	,U_NoteForLogistic	,Comments,U_VoucherTypeID,CAST(ObjType AS INT) AS ObjType,CAST(U_StatusID AS INT) AS U_StatusID
  FROM [PGI_UAT].[dbo].[ORDR] 
  WHERE 1 = 1
	AND CardCode NOT IN ('PGI_LOCK','PGI')
	-- return 12 monthAND DocDate >= DATEADD(month, -12, DATEADD(day, 1, EOMONTH(CURRENT_TIMESTAMP, -1)))
	--AND DocDate>= '2023-05-01'
),
S_RDR1 AS (
  SELECT DocEntry,LineNum,TargetType,TrgetEntry	,BaseType	,LineStatus	,InvntSttus,ItemCode	,Dscription	,Quantity	,ShipDate	,Price		,Currency	,LineTotal	,DocDate		,GrssProfit	,unitMsr		,ShipToCode	,ShipToDesc	,OcrCode3	,CogsOcrCo2	,U_S1_BaseNotes	,U_OrigiPrice	,U_DiscAmt	,U_DiscPrcnt	,U_PriceDisc	,WhsCode		,BaseCard	,TaxCode		,U_VoucherCode,VendorNum	,GTotal		,StockPrice	,StockValue	,'Pending_kho' AS voucher_type		
  FROM [PGI_UAT].[dbo].[RDR1] AS tb1
  WHERE 1 = 1 AND LineStatus = N'O'
	-- return 12 monthAND DocDate >= DATEADD(month, -12, DATEADD(day, 1, EOMONTH(CURRENT_TIMESTAMP, -1)))
  --  AND DocDate>= '2023-05-01'
),
S_ODRF AS (
  SELECT DocEntry	,DocNum	,DocType	,CANCELED	,DocStatus	,DocDueDate	,DocTotal	,GrosProfit	,JrnlMemo	,DocTotalSy	,PaidSys	,GrosProfSy	,UpdateDate	,CreateDate	,CreateTs ,TaxDate	,ReqDate	,ExtraDays	,SlpCode	,U_S1No   	,U_CardCode	,U_CardName	,U_NoteForAll,U_NoteForAcc	,U_NoteForWhs	,U_NoteForStatus	,U_Store	,U_TotalQty	,U_NoteForLogistic	,Comments,U_VoucherTypeID,CAST(ObjType AS INT) AS ObjType,CAST(U_StatusID AS INT) U_StatusID
  FROM [PGI_UAT].[dbo].[ODRF]
  WHERE 1 = 1
	AND CardCode NOT IN ('PGI_LOCK','PGI','PRO_MKT')AND DocStatus = N'O'
	AND ISNULL(ObjType,'') <> '15'
	-- return 12 monthAND DocDate >= DATEADD(month, -12, DATEADD(day, 1, EOMONTH(CURRENT_TIMESTAMP, -1)))--AND DocDate>= '2023-05-01'
),
S_DRF1 AS (
  SELECT DocEntry	,LineNum	,TargetType	,TrgetEntry	,BaseType	,LineStatus	,InvntSttus,ItemCode	,Dscription	,Quantity	,ShipDate	,Price	,Currency	,LineTotal	,DocDate	,GrssProfit	,unitMsr	,ShipToCode	,ShipToDesc	,OcrCode3	,CogsOcrCo2	,U_S1_BaseNotes	,U_OrigiPrice	,U_DiscAmt	,U_DiscPrcnt	,U_PriceDisc	,WhsCode	,BaseCard	,TaxCode	,U_VoucherCode,VendorNum,GTotal,StockPrice,StockValue,'Pending_ketoan' AS voucher_type		
  FROM [PGI_UAT].[dbo].[DRF1] AS tb1
  WHERE 1 = 1
	AND ISNULL(tb1.basetype, -1) <> '17'
	-- return 12 monthAND DocDate >= DATEADD(month, -12, DATEADD(day, 1, EOMONTH(CURRENT_TIMESTAMP, -1)))--AND DocDate>= '2023-05-01'
),
S_OINV AS (
  SELECT DocEntry	,DocNum	,DocType	,CANCELED	,DocStatus	,DocDueDate	,DocTotal	,GrosProfit	,JrnlMemo	,DocTotalSy	,PaidSys	,GrosProfSy	,UpdateDate	,CreateDate	,CreateTs ,TaxDate	,ReqDate	,ExtraDays	,SlpCode	,U_S1No   	,U_CardCode	,U_CardName	,U_NoteForAll,U_NoteForAcc	,U_NoteForWhs	,U_NoteForStatus	,U_Store	,U_TotalQty	,U_NoteForLogistic	,Comments,U_VoucherTypeID,CAST(ObjType AS INT) AS ObjType,CAST(U_StatusID AS INT) AS U_StatusID
  FROM [PGI_UAT].[dbo].[OINV]
  WHERE 1 = 1-- return 12 monthAND DocDate >= DATEADD(month, -12, DATEADD(day, 1, EOMONTH(CURRENT_TIMESTAMP, -1)))
	--AND DocDate >= '2023-05-01'
),
S_INV1 AS (
  SELECT tb1.DocEntry	,tb1.LineNum	,tb1.TargetType	,tb1.TrgetEntry	,tb1.BaseType	,COALESCE(tb2.LineStatus,tb1.LineStatus) AS LineStatus,tb1.InvntSttus,tb1.ItemCode	,tb1.Dscription	,tb1.Quantity	,tb1.ShipDate	,tb1.Price	,tb1.Currency	,tb1.LineTotal	,tb1.DocDate	,tb1.GrssProfit	,tb1.unitMsr	,tb1.ShipToCode	,tb1.ShipToDesc	,tb1.OcrCode3	,tb1.CogsOcrCo2	,tb1.U_S1_BaseNotes	,tb1.U_OrigiPrice	,tb1.U_DiscAmt	,tb1.U_DiscPrcnt	,tb1.U_PriceDisc	,tb1.WhsCode	,tb1.BaseCard	,tb1.TaxCode	,tb1.U_VoucherCode,tb1.VendorNum,tb1.GTotal,tb1.StockPrice,tb1.StockValue,'Sale Order' AS voucher_type		
  FROM [PGI_UAT].[dbo].[INV1] AS tb1
  LEFT JOIN [PGI_UAT].[dbo].[RDR1] AS tb2 ON tb1.BaseEntry = tb2.DocEntry
	  AND tb1.linenum = tb2.linenum
  WHERE 1 = 1
  -- return 12 monthAND tb1.DocDate >= DATEADD(month, -12, DATEADD(day, 1, EOMONTH(CURRENT_TIMESTAMP, -1)))--AND tb1.DocDate >= '2023-05-01'
),
S_ORDN AS (
  SELECT DocEntry	,DocNum	,DocType	,CANCELED	,DocStatus	,DocDueDate	,DocTotal	,GrosProfit	,JrnlMemo	,DocTotalSy	,PaidSys	,GrosProfSy	,UpdateDate	,CreateDate	,CreateTs ,TaxDate	,ReqDate	,ExtraDays	,SlpCode	,U_S1No	,U_CardCode	,U_CardName	,U_NoteForAll,U_NoteForAcc	,U_NoteForWhs	,U_NoteForStatus	,U_Store	,U_TotalQty	,U_NoteForLogistic	,Comments,U_VoucherTypeID,CAST(ObjType AS INT) AS ObjType,CAST(U_StatusID AS INT) AS U_StatusID
  FROM [PGI_UAT].[dbo].[ORDN]
  WHERE 1 = 1-- return 12 monthAND DocDate >= DATEADD(month, -12, DATEADD(day, 1, EOMONTH(CURRENT_TIMESTAMP, -1)))
	--AND DocDate >='2023-05-01'
),
S_RDN1 AS (
  SELECT DocEntry	,LineNum	,TargetType	,TrgetEntry	,BaseType	,LineStatus	,InvntSttus,ItemCode	,Dscription	,- Quantity AS Quantity	,ShipDate	,Price	,Currency	,-LineTotal AS LineTotal 	,DocDate	,GrssProfit	,unitMsr	,ShipToCode	,ShipToDesc	,OcrCode3	,CogsOcrCo2	,U_S1_BaseNotes	,U_OrigiPrice	,U_DiscAmt	,U_DiscPrcnt	,U_PriceDisc	,WhsCode	,BaseCard	,TaxCode	,U_VoucherCode,VendorNum,GTotal,StockPrice,StockValue,'Sale Return' AS voucher_type	
  FROM [PGI_UAT].[dbo].[RDN1]
  WHERE 1 = 1-- return 12 monthAND DocDate >= DATEADD(month, -12, DATEADD(day, 1, EOMONTH(CURRENT_TIMESTAMP, -1)))--AND DocDate >= '2023-05-01'
),
S_ORIN AS (
	SELECT DocEntry,DocNum,DocType,CANCELED,DocStatus	,DocDueDate	,case CANCELED when 'Y' then DocTotal else -DocTotal end DocTotal	,case CANCELED when 'Y' then GrosProfit else -GrosProfit end GrosProfit	,JrnlMemo, case CANCELED when 'Y' then DocTotalSy else -DocTotalSy end  DocTotalSy,
  case CANCELED when 'Y' then PaidSys else -PaidSys end  PaidSys,case CANCELED when 'Y' then GrosProfSy else -GrosProfSy end  GrosProfSy,UpdateDate	,CreateDate	,CreateTs ,TaxDate,ReqDate,ExtraDays,SlpCode,U_S1No	,U_CardCode	,U_CardName	,U_NoteForAll,U_NoteForAcc	,U_NoteForWhs	,U_NoteForStatus	,U_Store	,U_TotalQty	,U_NoteForLogistic	,Comments,U_VoucherTypeID,CAST(ObjType AS INT) AS ObjType,CAST(U_StatusID AS INT) AS U_StatusID
  FROM [PGI_UAT].[dbo].[ORIN] WHERE 1 = 1
  --SELECT DocEntry	,DocNum	,DocType	,CANCELED	,DocStatus	,DocDueDate	,DocTotal	,GrosProfit	,JrnlMemo	,DocTotalSy	,PaidSys	,GrosProfSy	,UpdateDate	,CreateDate	,CreateTs ,TaxDate	,ReqDate	,ExtraDays	,SlpCode	,U_S1No	,U_CardCode	,U_CardName	,U_NoteForAll,U_NoteForAcc	,U_NoteForWhs	,U_NoteForStatus	,U_Store	,U_TotalQty	,U_NoteForLogistic	,Comments,U_VoucherTypeID,CAST(ObjType AS INT) AS ObjType,CAST(U_StatusID AS INT) AS U_StatusID
  --FROM [PGI_UAT].[dbo].[ORIN]
  --WHERE 1 = 1-- return 12 monthAND DocDate >= DATEADD(month, -12, DATEADD(day, 1, EOMONTH(CURRENT_TIMESTAMP, -1)))--AND DocDate >= '2023-05-01'

),
S_RIN1 AS (
SELECT DocEntry	,LineNum,TargetType	,TrgetEntry	,BaseType,LineStatus,InvntSttus,ItemCode,Dscription,case TargetType when 14 then Quantity else -Quantity end	AS Quantity,ShipDate	
,Price,Currency,case TargetType when 14  then LineTotal else -LineTotal end AS LineTotal,DocDate,case TargetType when 14 then GrssProfit else -GrssProfit end GrssProfit	,unitMsr	,
ShipToCode,ShipToDesc,OcrCode3,CogsOcrCo2,U_S1_BaseNotes,case TargetType when 14 then U_OrigiPrice else -U_OrigiPrice end  U_OrigiPrice	,case TargetType when 14 then U_DiscAmt else -U_DiscAmt end U_DiscAmt,case TargetType when 14 then U_DiscPrcnt else -U_DiscPrcnt end U_DiscPrcnt	
,case TargetType when 14 then U_PriceDisc else -U_PriceDisc end U_PriceDisc	,WhsCode,BaseCard,TaxCode,U_VoucherCode,VendorNum,case TargetType when 14 then GTotal else -GTotal end GTotal,case TargetType when 14 then StockPrice else -StockPrice end StockPrice,
case TargetType when 14 then StockValue else -StockValue end StockValue,'Sale Order' AS voucher_type		
FROM [PGI_UAT].[dbo].[RIN1] WHERE 1 = 1
  --SELECT DocEntry	,LineNum	,TargetType	,TrgetEntry	,BaseType	,LineStatus	,InvntSttus,ItemCode	,Dscription	,-Quantity	AS Quantity,ShipDate	,Price	,Currency	,-LineTotal AS LineTotal	,DocDate	,GrssProfit	,unitMsr	,ShipToCode	,ShipToDesc	,OcrCode3	,CogsOcrCo2	,U_S1_BaseNotes	,U_OrigiPrice	,U_DiscAmt	,U_DiscPrcnt	,U_PriceDisc	,WhsCode	,BaseCard	,TaxCode	,U_VoucherCode,VendorNum,GTotal,StockPrice,StockValue,'Sale Order' AS voucher_type		
  --FROM [PGI_UAT].[dbo].[RIN1]
  --WHERE 1 = 1-- return 12 monthAND DocDate >= DATEADD(month, -12, DATEADD(day, 1, EOMONTH(CURRENT_TIMESTAMP, -1)))
	--AND DocDate >= '2023-05-01'
),
S_OWHS AS (
  SELECT WhsCode	,WhsName	
  FROM [PGI_UAT].[dbo].[OWHS] 
),
S_OSLP AS (
  SELECT SlpCode	,SlpName	
  FROM [PGI_UAT].[dbo].[OSLP] 
),
S_OCRD AS (
  SELECT CardCode,CardName AS Main_CardName,U_TerrDesc,ChannlBP	 ,U_TerrID
  FROM [PGI_UAT].[dbo].[OCRD] 
),
S_OITB AS (
  SELECT ItmsGrpCod	,ItmsGrpNam	,U_CostAct1	,U_ReportGroup
  FROM [PGI_UAT].[dbo].[OITB]
),
S_OMRC AS (
  SELECT FirmCode,FirmName	
  FROM [PGI_UAT].[dbo].[OMRC]
),
S_OITM AS (
  SELECT ItemCode	,ItmsGrpCod	,FirmCode	,U_ItmGrp1	,U_ItmGrp2
  FROM [PGI_UAT].[dbo].[OITM]
),
S_VOUCHERTYPE AS (
  SELECT Code AS vouchercode	,Name	
  FROM [PGI_UAT].[dbo].[@VOUCHERTYPE]
),
S_S1_Region AS (
  SELECT *
  FROM [PGI_UAT].[dbo].[S1_Region]
),
S_S1_ObjType AS (
  SELECT CAST(Code AS INT) AS Code,
	[Name] AS objname
  FROM [PGI_UAT].[dbo].[S1_ObjType]
),
S_S1_DocStatus AS (
  SELECT CAST(Code AS INT) AS Code_doc,
	[Name] AS StatusName
  FROM [PGI_UAT].[dbo].[@DocStatus]
),
T_Colab_Order AS (
  SELECT 
	SOO.DocEntry	,SOO.LineNum	,SOO.TargetType	,SOO.TrgetEntry	,SOO.BaseType	,SOO.LineStatus	,SOO.InvntSttus,SOO.ItemCode	,SOO.Dscription	,SOO.Quantity	,SOO.ShipDate	,SOO.Price	,SOO.Currency	,SOO.LineTotal	,SOO.DocDate	,SOO.GrssProfit	,SOO.unitMsr	,SOO.ShipToCode	,SOO.ShipToDesc	,SOO.OcrCode3	,SOO.CogsOcrCo2	,SOO.U_S1_BaseNotes	,SOO.U_OrigiPrice	,SOO.U_DiscAmt	,SOO.U_DiscPrcnt	,SOO.U_PriceDisc	,SOO.WhsCode	,SOO.BaseCard	,SOO.TaxCode	,SOO.U_VoucherCode,SOO.VendorNum,SOO.GTotal,SOO.StockPrice,SOO.StockValue,SOO.voucher_type,
	SOD.DocNum	,SOD.DocType	,SOD.CANCELED	,SOD.DocStatus	,SOD.DocDueDate	,SOD.DocTotal	,SOD.GrosProfit	,SOD.JrnlMemo	,SOD.DocTotalSy	,SOD.PaidSys	,SOD.GrosProfSy	,SOD.UpdateDate	,SOD.CreateDate	,SOD.CreateTs,SOD.TaxDate	,SOD.ReqDate	,SOD.ExtraDays	,SOD.SlpCode	,SOD.U_S1No   	,SOD.U_CardCode	,SOD.U_CardName	,SOD.U_NoteForAll,SOD.U_NoteForAcc	,SOD.U_NoteForWhs	,SOD.U_NoteForStatus	,SOD.U_Store	,SOD.U_TotalQty	,SOD.U_NoteForLogistic	,SOD.Comments,SOD.U_VoucherTypeID,SOD.ObjType,SOD.U_StatusID
  FROM S_RDR1 AS SOO
  INNER JOIN S_ORDR AS SOD ON SOO.DocEntry = SOD.DocEntry
),

T_Colab_Invoice AS (
  SELECT 	SIV.DocEntry	,SIV.LineNum	,SIV.TargetType	,SIV.TrgetEntry	,SIV.BaseType	,SIV.LineStatus	,SIV.InvntSttus,SIV.ItemCode	,SIV.Dscription	,SIV.Quantity	,SIV.ShipDate	,SIV.Price	,SIV.Currency	,SIV.LineTotal	,SIV.DocDate	,SIV.GrssProfit	,SIV.unitMsr	,SIV.ShipToCode	,SIV.ShipToDesc	,SIV.OcrCode3	,SIV.CogsOcrCo2	,SIV.U_S1_BaseNotes	,SIV.U_OrigiPrice	,SIV.U_DiscAmt	,SIV.U_DiscPrcnt	,SIV.U_PriceDisc	,SIV.WhsCode	,SIV.BaseCard	,SIV.TaxCode	,SIV.U_VoucherCode,SIV.VendorNum,SIV.GTotal,SIV.StockPrice,SIV.StockValue,SIV.voucher_type,
	SOI.DocNum	,SOI.DocType	,SOI.CANCELED	,SOI.DocStatus	,SOI.DocDueDate	,SOI.DocTotal	,SOI.GrosProfit	,SOI.JrnlMemo	,SOI.DocTotalSy	,SOI.PaidSys	,SOI.GrosProfSy	,SOI.UpdateDate	,SOI.CreateDate	,SOI.CreateTs,SOI.TaxDate	,SOI.ReqDate	,SOI.ExtraDays	,SOI.SlpCode	,SOI.U_S1No   	,SOI.U_CardCode	,SOI.U_CardName	,SOI.U_NoteForAll,SOI.U_NoteForAcc	,SOI.U_NoteForWhs	,SOI.U_NoteForStatus	,SOI.U_Store	,SOI.U_TotalQty	,SOI.U_NoteForLogistic	,SOI.Comments,SOI.U_VoucherTypeID,SOI.ObjType,SOI.U_StatusID
  FROM S_INV1 AS SIV
  INNER JOIN S_OINV AS SOI ON SIV.DocEntry = SOI.DocEntry
),

T_Colab_Return AS (
  SELECT SRD.DocEntry	,SRD.LineNum	,SRD.TargetType	,SRD.TrgetEntry	,SRD.BaseType	,SRD.LineStatus	,SRD.InvntSttus,SRD.ItemCode	,SRD.Dscription	,SRD.Quantity	,SRD.ShipDate	,SRD.Price	,SRD.Currency	,SRD.LineTotal	,SRD.DocDate	,SRD.GrssProfit	,SRD.unitMsr	,SRD.ShipToCode	,SRD.ShipToDesc	,SRD.OcrCode3	,SRD.CogsOcrCo2	,SRD.U_S1_BaseNotes	,SRD.U_OrigiPrice	,SRD.U_DiscAmt	,SRD.U_DiscPrcnt	,SRD.U_PriceDisc	,SRD.WhsCode	,SRD.BaseCard	,SRD.TaxCode	,SRD.U_VoucherCode,SRD.VendorNum,SRD.GTotal,SRD.StockPrice,SRD.StockValue,SRD.voucher_type,
	SOR.DocNum	,SOR.DocType	,SOR.CANCELED	,SOR.DocStatus	,SOR.DocDueDate	,SOR.DocTotal	,SOR.GrosProfit	,SOR.JrnlMemo	,SOR.DocTotalSy	,SOR.PaidSys	,SOR.GrosProfSy	,SOR.UpdateDate	,SOR.CreateDate	,SOR.CreateTs,SOR.TaxDate	,SOR.ReqDate	,SOR.ExtraDays	,SOR.SlpCode	,SOR.U_S1No   	,SOR.U_CardCode	,SOR.U_CardName	,SOR.U_NoteForAll,SOR.U_NoteForAcc	,SOR.U_NoteForWhs	,SOR.U_NoteForStatus	,SOR.U_Store	,SOR.U_TotalQty	,SOR.U_NoteForLogistic	,SOR.Comments,SOR.U_VoucherTypeID,SOR.ObjType,SOR.U_StatusID
  FROM S_RDN1 AS SRD
  INNER JOIN S_ORDN AS SOR ON SRD.DocEntry = SOR.DocEntry
),

T_Colab_Return_1 AS (
  SELECT 	SRI.DocEntry	,SRI.LineNum	,SRI.TargetType	,SRI.TrgetEntry	,SRI.BaseType	,SRI.LineStatus	,SRI.InvntSttus,SRI.ItemCode	,SRI.Dscription	,SRI.Quantity	,SRI.ShipDate	,SRI.Price	,SRI.Currency	,SRI.LineTotal	,SRI.DocDate	,SRI.GrssProfit	,SRI.unitMsr	,SRI.ShipToCode	,SRI.ShipToDesc	,SRI.OcrCode3	,SRI.CogsOcrCo2	,SRI.U_S1_BaseNotes	,SRI.U_OrigiPrice	,SRI.U_DiscAmt	,SRI.U_DiscPrcnt	,SRI.U_PriceDisc	,SRI.WhsCode	,SRI.BaseCard	,SRI.TaxCode	,SRI.U_VoucherCode,SRI.VendorNum,SRI.GTotal,SRI.StockPrice,SRI.StockValue,SRI.voucher_type,
	SOR.DocNum	,SOR.DocType	,SOR.CANCELED	,SOR.DocStatus	,SOR.DocDueDate	,SOR.DocTotal	,SOR.GrosProfit	,SOR.JrnlMemo	,SOR.DocTotalSy	,SOR.PaidSys	,SOR.GrosProfSy	,SOR.UpdateDate	,SOR.CreateDate	,SOR.CreateTs,SOR.TaxDate	,SOR.ReqDate	,SOR.ExtraDays	,SOR.SlpCode	,SOR.U_S1No   	,SOR.U_CardCode	,SOR.U_CardName	,SOR.U_NoteForAll,SOR.U_NoteForAcc	,SOR.U_NoteForWhs	,SOR.U_NoteForStatus	,SOR.U_Store	,SOR.U_TotalQty	,SOR.U_NoteForLogistic	,SOR.Comments,SOR.U_VoucherTypeID,SOR.ObjType,SOR.U_StatusID
  FROM S_RIN1 AS SRI
  INNER JOIN S_ORIN AS SOR ON SRI.DocEntry = SOR.DocEntry
),

T_Colab_Pending AS (
  SELECT SDR.DocEntry	,SDR.LineNum	,SDR.TargetType	,SDR.TrgetEntry	,SDR.BaseType	,SDR.LineStatus	,SDR.InvntSttus,SDR.ItemCode	,SDR.Dscription	,SDR.Quantity	,SDR.ShipDate	,SDR.Price	,SDR.Currency	,SDR.LineTotal	,SDR.DocDate	,SDR.GrssProfit	,SDR.unitMsr	,SDR.ShipToCode	,SDR.ShipToDesc	,SDR.OcrCode3	,SDR.CogsOcrCo2	,SDR.U_S1_BaseNotes	,SDR.U_OrigiPrice	,SDR.U_DiscAmt	,SDR.U_DiscPrcnt	,SDR.U_PriceDisc	,SDR.WhsCode	,SDR.BaseCard	,SDR.TaxCode	,SDR.U_VoucherCode,SDR.VendorNum,SDR.GTotal,SDR.StockPrice,SDR.StockValue,SDR.voucher_type,
	SOD.DocNum	,SOD.DocType	,SOD.CANCELED	,SOD.DocStatus	,SOD.DocDueDate	,SOD.DocTotal	,SOD.GrosProfit	,SOD.JrnlMemo	,SOD.DocTotalSy	,SOD.PaidSys	,SOD.GrosProfSy	,SOD.UpdateDate	,SOD.CreateDate	,SOD.CreateTs,SOD.TaxDate	,SOD.ReqDate	,SOD.ExtraDays	,SOD.SlpCode	,SOD.U_S1No   	,SOD.U_CardCode	,SOD.U_CardName	,SOD.U_NoteForAll,SOD.U_NoteForAcc	,SOD.U_NoteForWhs	,SOD.U_NoteForStatus	,SOD.U_Store	,SOD.U_TotalQty	,SOD.U_NoteForLogistic	,SOD.Comments,SOD.U_VoucherTypeID,SOD.ObjType,SOD.U_StatusID
  FROM S_DRF1 AS SDR
  INNER JOIN S_ODRF AS SOD ON SDR.DocEntry = SOD.DocEntry

),
T_Union_Data AS (
  SELECT * FROM T_Colab_Order
  UNION ALL
  SELECT * FROM T_Colab_Invoice
  UNION ALL
  SELECT * FROM T_Colab_Return
  UNION ALL
  SELECT * FROM T_Colab_Return_1
  UNION ALL
  SELECT * FROM T_Colab_Pending

),
T_Mapping_Info_Step1 AS (
  SELECT
	TUD.DocEntry	AS docentry,TUD.LineNum		AS linenum,TUD.TargetType	AS targettype,TUD.TrgetEntry	AS trgetentry,TUD.BaseType	AS basetype,TUD.LineStatus	AS linestatus,TUD.InvntSttus	AS invntsttus,TUD.ItemCode	AS itemcode,TUD.Dscription	AS dscription,TUD.Quantity	AS quantity,TUD.ShipDate	AS shipdate,TUD.Price		AS price,TUD.Currency	AS currency,TUD.LineTotal	AS linetotal,TUD.DocDate 	AS docdate,TUD.GrssProfit	AS grssprofit,TUD.unitMsr		AS unitmsr,TUD.ShipToCode	AS shiptocode,TUD.ShipToDesc	AS shiptodesc,TUD.OcrCode3	AS ocrcode3,TUD.CogsOcrCo2	AS cogsocrco2,TUD.U_S1_BaseNotes	AS u_s1_basenotes,TUD.U_OrigiPrice	AS u_origiprice,TUD.U_DiscAmt	AS u_discamt,TUD.U_DiscPrcnt	AS u_discprcnt,TUD.U_PriceDisc	AS u_pricedisc,TUD.WhsCode		AS whscode,TUD.BaseCard	AS basecard,TUD.TaxCode		AS taxcode,TUD.U_VoucherCode AS u_vouchercode,TUD.VendorNum	AS vendornum,TUD.GTotal		AS gtotal,TUD.StockPrice	AS stockprice,TUD.StockValue	AS stockvalue,TUD.voucher_type  AS voucher_type,
	TUD.DocNum		AS docnum,TUD.DocType		AS doctype,TUD.CANCELED	AS canceled,TUD.DocStatus	AS docstatus,TUD.DocDueDate	AS docduedate,TUD.DocTotal	AS doctotal,TUD.GrosProfit	AS grosprofit,TUD.JrnlMemo	AS jrnlmemo,TUD.DocTotalSy	AS doctotalsy,TUD.PaidSys		AS paidsys,TUD.GrosProfSy	AS grosprofsy,TUD.UpdateDate  AS updatedate,TUD.CreateDate	AS createdate,TUD.CreateTs	AS createts,TUD.TaxDate		AS taxdate,TUD.ReqDate		AS reqdate,TUD.ExtraDays	AS extradays,TUD.SlpCode		AS slpcode,TUD.U_S1No   	AS u_s1no,TUD.U_CardCode	AS u_cardcode,TUD.U_CardName	AS u_cardname,TUD.U_NoteForAll  AS u_noteforall,TUD.U_NoteForAcc  AS u_noteforacc,TUD.U_NoteForWhs  AS u_noteforwhs,TUD.U_NoteForStatus	AS u_noteforstatus,TUD.U_Store		AS u_store,TUD.U_TotalQty	AS u_totalqty,TUD.U_NoteForLogistic AS u_noteforlogistic,TUD.Comments	AS comments,TUD.U_VoucherTypeID  AS u_vouchertypeid,TUD.ObjType		AS objtype,TUD.U_StatusID	AS u_statusid,
	SDW.WhsName		AS whsname,
	SOS.SlpName		AS slpname,
	SOC.CardCode      AS cardcode,
	SOC.Main_CardName	 AS main_cardname,SOC.U_TerrDesc	AS u_terrdesc,SOC.ChannlBP	AS channlbp,SOC.U_TerrID	AS u_terrid,
	SOM.ItmsGrpCod	AS itmsgrpcod,SOM.FirmCode	AS firmcode,SOM.U_ItmGrp1	AS u_itmgrp1,SOM.U_ItmGrp2	AS u_itmgrp2,
	SOB.ItmsGrpNam	AS itmsgrpnam,SOB.U_CostAct1	AS u_costact1,SOB.U_ReportGroup	  AS u_reportgroup,
	SRC.FirmName	AS firmname,
	SSR.Territory      AS territory,
	SSR.Province	AS province,
	SSR.Area_Code	    AS area_code,
	SSR.Area		AS area,
	SSR.Region_Code	AS region_code,
	SSR.Region		AS region,
	SSO.Code               AS code,
	SSO.objname		AS objname,
	SSD.Code_doc           AS code_doc,
	SSD.StatusName	AS statusname,
	SVC.vouchercode        AS vouchercode,
	SVC.Name	    	   AS name,
	CURRENT_TIMESTAMP      AS updated_time,
	0                      AS __index_level_0__
  FROM T_Union_Data AS TUD
  LEFT JOIN S_OWHS AS SDW ON TUD.WhsCode = SDW.WhsCode
  LEFT JOIN S_OSLP AS SOS ON TUD.SlpCode = SOS.SlpCode
  LEFT JOIN S_OCRD AS SOC ON TUD.U_CardCode = SOC.CardCode
  LEFT JOIN S_OITM AS SOM ON TUD.ItemCode = SOM.ItemCode
  LEFT JOIN S_OITB AS SOB ON SOM.ItmsGrpCod = SOB.ItmsGrpCod
  LEFT JOIN S_OMRC AS SRC ON SOM.FirmCode = SRC.FirmCode
  LEFT JOIN S_S1_Region AS SSR ON SOC.U_TerrID = SSR.Territory
  INNER JOIN S_S1_ObjType AS SSO ON TUD.ObjType = SSO.Code
  LEFT JOIN S_s1_DocStatus AS SSD ON TUD.U_StatusID = SSD.Code_doc
  LEFT JOIN S_VOUCHERTYPE AS SVC ON TUD.U_VoucherTypeID = SVC.vouchercode
),
Final AS (
  SELECT * FROM T_Mapping_Info_Step1
)
SELECT *-- docentry,linenum,targettype,trgetentry,basetype,linestatus,invntsttus,itemcode,dscription,quantity,shipdate,price,currency,linetotal,docdate,grssprofit,unitmsr,shiptocode,shiptodesc,ocrcode3,cogsocrco2,u_s1_basenotes,u_origiprice,u_discamt,u_discprcnt,u_pricedisc,whscode,basecard,taxcode,u_vouchercode,vendornum,gtotal,stockprice,stockvalue,voucher_type,docnum,doctype,canceled,docstatus,docduedate,doctotal,grosprofit
 FROM Final
WHERE U_S1No in ('PGIO-013121')
-- u_s1_basenotes in ('PGIO-047447','PGIO-047667','PGIO-017385','PGIO-047415','PGIO-047684','PGIO-046740','PGIO-062477','PGIO-062524','PGIO-062105','PGIO-040108','PGIO-007055','PGIO-053524','PGIO-030437 / PGIO-057479','PGIO-054846','PGIO-030795','PGIO-026248','PGIO-026255','PGIO-056334','PGIO-056339','PGIO-056341','PGIO-062484','PGIO-050762','PGIO-055527','PGIO-044461','PGIO-028138 / PGIO-050610 / PGIO-050610 / PGIO-050610','PGIO-039546','PGIM-049176','PGIO-059971','PGIO-013454','PGIO-031121','PGIO-029787','PGIO-062555','PGIO-020956','PGIO-042073','PGIO-037533','PGIO-062556','PGIO-014526','PGIO-045735','PGIO-043628','PGIO-032442','PGIO-037771','PGIO-028138','PGIO-028136 / PGIO-045284 / PGIO-045284 / PGIO-045284','PGIO-020667','PGIO-013738 / PGIO-031922 / PGIO-031922 / PGIO-031922','PGIO-028136','PGIN-068164','PGIO-053496','PGIO-062562','PGIO-062560','PGIO-062564','PGIO-054668','PGIO-054671','PGIO-047876','PGIN-070655','PGIO-046512','PGIO-062240','PGIO-010893','PGIO-014027','PGIO-005836','PGIO-012052','PGIO-014027 / PGIO-037731 / PGIO-037731 / PGIO-037731','PGIO-006016','PGIO-014103','PGIO-053390','PGIO-053388','PGIO-052206','PGIO-041186','PGIO-048085','PGIO-053952','PGIO-051480','PGIO-062553','PGIO-062569','PGIO-053697','PGIO-062554','PGIO-062571','PGIO-062570','PGIO-062567','PGIO-062568','PGIO-062565','PGIO-062558','PGIO-062561','PGIO-062566','PGIO-062563','PGIO-029799','PGIO-039629','PGIO-029794','PGIO-037666','PGIO-037665','PGIO-029797','PGIO-037671','PGIO-062559','PGIO-035136','PGIO-043008','PGIO-053044','PGIN-031115','PGIO-058421','PGIO-017308','PGIO-053110','PGIO-048894','PGIO-053511','PGIO-051686','PGIO-045080','PGIO-025202','PGIO-006866 / PGIO-015793 / PGIO-015793 / PGIO-015793','PGIO-054741','PGIO-054419','PGIO-052274','PGIO-012238','PGIN-028687','PGIO-033278','PGIN-054845','PGIO-010647','PGIO-016020','PGIO-038078','PGIN-070196','PGIO-016132','PGIO-029773','PGIO-032635','PGIO-032801','PGIO-012339','PGIN-070211','PGIN-054817','PGIO-032863','PGIO-004322','PGIO-034916','PGIO-004135','PGIO-033951','PGIN-054530','PGIO-034834','PGIN-042574','PGIO-034857','PGIO-012312','PGIN-055379','PGIO-015654','PGIN-038493','PGIO-012233','PGIN-055359','PGIN-054445','PGIO-013575','PGIN-047594','PGIO-033443','PGIN-070204','PGIN-047602','PGIN-061324','PGIO-016293','PGIN-054959','PGIN-061286','PGIO-032952','PGIO-044745','PGIO-016024','PGIN-038686','PGIN-038563','PGIO-016023','PGIN-063207','PGIO-017170','PGIO-039090','PGIN-038566','PGIO-009491','PGIN-038643','PGIN-047754','PGIN-038690','PGIO-016137','PGIN-063699','PGIN-038532','PGIO-038427','PGIO-033936','PGIN-048122','PGIO-032833','PGIO-038489','PGIO-016134','PGIO-032846','PGIO-033928','PGIO-046700','PGIO-046675','PGIO-016019','PGIO-017375','PGIN-038556','PGIO-033609','PGIO-046728','PGIO-015641','PGIO-046440','PGIO-033611','PGIO-010664','PGIN-054826','PGIO-036291','PGIN-062610','PGIN-038570','PGIN-038555','PGIN-013035','PGIN-042124','PGIN-070125','PGIO-030550','PGIN-042025','PGIN-047606','PGIN-038697','PGIN-047589','PGIO-030549','PGIN-048114','PGIO-005819','PGIN-047595','PGIN-047747','PGIO-029669','PGIN-047587','PGIO-031983','PGIN-038713','PGIN-038709','PGIN-054507','PGIO-025698','PGIN-048158','PGIO-034861','PGIO-051494','PGIO-012321','PGIO-032799','PGIO-033425','PGIO-016129','PGIO-015964','PGIN-038569','PGIN-047757','PGIO-029778','PGIO-016016','PGIN-047755','PGIO-041068','PGIO-015953','PGIN-047773','PGIO-024304','PGIO-051649','PGIO-049667','PGIO-054068','PGIO-053334','PGIO-053241','PGIO-053330','PGIO-053326','PGIO-051848','PGIO-052994','PGIO-053112','PGIO-054262','PGIO-050026','PGIO-051854','PGIO-052183','PGIO-038373','PGIO-049728','PGIO-041572','PGIO-040633','PGIO-047457','PGIO-036595','PGIO-053638','PGIO-053212','PGIO-051142','PGIO-049688','PGIO-053346','PGIO-053804','PGIO-051729','PGIO-046465','PGIO-053161','PGIO-049394','PGIO-046042','PGIO-049318','PGIO-041772','PGIO-050926','PGIO-053844','PGIO-054116','PGIO-051602','PGIO-053137','PGIO-048436','PGIO-040706','PGIO-051714','PGIO-049286','PGIO-050350','PGIO-052138','PGIO-051720','PGIO-050847','PGIO-050526','PGIO-053834','PGIO-052046','PGIO-053404','PGIO-053621','PGIO-053828','PGIO-054373','PGIO-054375','PGIO-054468','PGIO-052582','PGIO-054024','PGIO-053592','PGIO-045046','PGIO-053941','PGIO-053957','PGIN-055195','PGIO-047638','PGIO-039585','PGIO-054049','PGIO-062522','PGIO-062523'
--)
------('PGIO-063443','PGIO-062571','PGIO-062570','PGIO-062569','PGIO-062568','PGIO-062567','PGIO-062566','PGIO-062565','PGIO-062564','PGIO-062563','PGIO-062562','PGIO-062561','PGIO-062560','PGIO-062559','PGIO-062556','PGIO-062555','PGIO-062554','PGIO-062553','PGIO-062523','PGIO-062522','PGIO-062105','PGIO-042485','PGIO-042483','PGIO-042479','PGIO-041607')

----docdate between' 2024-10-01' and '2024-10-31'
