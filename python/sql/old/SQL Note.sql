drop table  dw-pgi.Financials.financial_templates

delete FROM `dw-pgi.Financials.balance_sheet` where 1=1
delete FROM `dw-pgi.Financials.cash_flow` where 1=1
delete FROM `dw-pgi.Financials.income_statement`where 1=1
delete FROM `dw-pgi.Financials.journal_entry` where 1=1
delete FROM 'dw-pgi.Inventory.whse_journal' where 1=1


SELECT * FROM `dw-pgi.Financials.cash_flow` 
SELECT * FROM `dw-pgi.Financials.cash_flow`
SELECT * FROM`dw-pgi.Financials.income_statement`

--Create table 
CREATE TABLE `dw-pgi.Financials.journal_entry`(
    TransId STRING,
    TransType STRING,
    RefDate DATE,
    Account STRING,
    ContraAct STRING,
    DebPayAcct STRING,
    LineMemo STRING,
    Debit FLOAT64,
    Credit FLOAT64,
    AcctName STRING,
    U_InvNo STRING,
    KhoanMuc STRING,
    NhanHang STRING,
    NhomHang STRING,
    DoiTac STRING,
    NhanVien STRING,
    LocationName STRING,
    Ref3Line STRING,
    Remarks STRING,
    Ref2 STRING,
    U_S1No STRING,
    U_VoucherNo STRING,
    SoCT STRING,
    FCDebit FLOAT64,
    FCCredit FLOAT64,
    FCCurrency STRING,
    U_NAME STRING,
    SystemDate TIMESTAMP,
    TaxDate DATE,
    U_AdjTran STRING,
    VatGroup STRING,
    BaseSum FLOAT64,
    U_BPCode STRING,
    U_BPName STRING,
    U_TaxCode STRING,
    BaseRef STRING
);

CREATE TABLE `dw-pgi.Inventory.whse_journal` (
    VoucherTypeName STRING,
    DocEntry STRING,
    S1No STRING,
    VoucherNo STRING,
    DocDate DATE,
    CreateDate DATE,
    ItemCode STRING,
    Dscription STRING,
    InQty FLOAT64,
    OutQty FLOAT64,
    Warehouse STRING,
    ItemCost FLOAT64,
    TransValue FLOAT64,
    InvntAct STRING,
    DiffAcc STRING,
    CardCode STRING,
    Comments STRING,
    UnitPrice FLOAT64,
    DocLineTotal FLOAT64,
    U_NAME STRING,
    ObjType STRING,
    JrnlMemo STRING
);

CREATE TABLE `dw-pgi.Sales.order_to_cash` (
    docentry INTEGER,
    linenum INTEGER,
    targettype FLOAT64,
    trgetentry FLOAT64,
    basetype INTEGER,
    linestatus STRING,
    invntsttus STRING,
    itemcode STRING,
    dscription STRING,
    quantity FLOAT64,
    shipdate TIMESTAMP,
    price FLOAT64,
    currency STRING,
    linetotal FLOAT64,
    docdate TIMESTAMP,
    grssprofit FLOAT64,
    unitmsr STRING,
    shiptocode STRING,
    shiptodesc STRING,
    ocrcode3 STRING,
    cogsocrco2 STRING,
    u_s1_basenotes STRING,
    u_origiprice FLOAT64,
    u_discamt FLOAT64,
    u_discprcnt FLOAT64,
    u_pricedisc FLOAT64,
    whscode STRING,
    basecard STRING,
    taxcode INTEGER,
    u_vouchercode STRING,
    vendornum STRING,
    gtotal FLOAT64,
    stockprice FLOAT64,
    stockvalue FLOAT64,
    voucher_type STRING,
    docnum INTEGER,
    doctype STRING,
    canceled STRING,
    docstatus STRING,
    docduedate TIMESTAMP,
    doctotal FLOAT64,
    grosprofit FLOAT64,
    jrnlmemo STRING,
    doctotalsy FLOAT64,
    paidsys FLOAT64,
    grosprofsy FLOAT64,
    updatedate TIMESTAMP,
    createdate TIMESTAMP,
    createts INTEGER,
    taxdate DATE,
    reqdate TIMESTAMP,
    extradays FLOAT64,
    slpcode INTEGER,
    u_s1no STRING,
    u_cardcode STRING,
    u_cardname STRING,
    u_noteforall STRING,
    u_noteforacc STRING,
    u_noteforwhs STRING,
    u_noteforstatus STRING,
    u_store STRING,
    u_totalqty FLOAT64,
    u_noteforlogistic STRING,
    comments STRING,
    u_vouchertypeid STRING,
    objtype INTEGER,
    u_statusid INTEGER,
    whsname STRING,
    slpname STRING,
    cardcode STRING,
    main_cardname STRING,
    u_terrdesc STRING,
    channlbp STRING,
    u_terrid INTEGER,
    itmsgrpcod FLOAT64,
    firmcode FLOAT64,
    u_itmgrp1 STRING,
    u_itmgrp2 STRING,
    itmsgrpnam STRING,
    u_costact1 STRING,
    u_reportgroup STRING,
    firmname STRING,
    territory INTEGER,
    province STRING,
    area_code INTEGER,
    area STRING,
    region_code INTEGER,
    region STRING,
    code INTEGER,
    objname STRING,
    code_doc INTEGER,
    statusname STRING,
    vouchercode STRING,
    name STRING,
    updated_time TIMESTAMP,
    __index_level_0__ INTEGER
);
CREATE TABLE dw-pgi.Financials.financial_templates (
    Temp_code STRING,
    Temp_name STRING,
    Name_org STRING,
    Name_frgn STRING,
    `No` INT64,
    Level INT64,
    FatherNum INT64,
    VisOrder INT64,
    Level3 STRING,
    Level2 STRING,
    Level1 STRING
);
CREATE TABLE `dw-pgi.Financials.stats_month_to_date` (
  SalesThisYear NUMERIC,
  GrossThisYear NUMERIC,
  SalesLastYear NUMERIC,
  GrossLastYear NUMERIC,
  SalesThisMonth NUMERIC,
  GrossThisMonth NUMERIC,
  SalesLastMonth NUMERIC,
  GrossLastMonth NUMERIC,
  InvoicesThisMonth INT64,
  InvoicesLastMonth INT64,
  CreditNotesThisMonth INT64,
  CreditNotesLastMonth INT64,
  InvoicesThisYear INT64,
  InvoicesLastYear INT64,
  CreditNotesThisYear INT64,
  CreditNotesLastYear INT64,
  Days INT64
);



CREATE TABLE [dbo].[BCDKT](
	[Account] [nvarchar](250) NULL,
	[FormatCode] [nvarchar](250) NULL,
	[Name] [nvarchar](max) NULL,
	[Name_Org] [nvarchar](max) NULL,
	[N0] [nvarchar](250) NULL,
	[N1] [nvarchar](250) NULL,
	[FrgnName] [nvarchar](max) NULL,
	[Levels] [smallint] NULL,
	[FatherNum] [smallint] NULL,
	[Active] [char](1) NULL,
	[HasSons] [char](1) NULL,
	[VisOrder] [money] NULL,
	[SubSum] [char](1) NULL,
	[SubName] [nvarchar](max) NULL,
	[visible] [bit] NULL,
	[LastDate] [money] NULL,
	[Atdate] [money] NULL,
	[Remarks1] [nvarchar](max) NULL,
	[Remarks2] [nvarchar](max) NULL,
	[CatId] [nvarchar](250) NULL,
	[AdjTranStr] [nvarchar](250) NULL,
	[PrintHeadr] [nvarchar](250) NULL,
	[CompnyAddr] [nvarchar](250) NULL,
	[FrDate] [datetime] NULL,
	[ToDate] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

/****** Object:  Table [dbo].[JournalEntry]    Script Date: 11/01/2024 12:01:43 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[JournalEntry](
	[TransId] [int] NULL,
	[TransType] [int] NULL,
	[RefDate] [datetime] NULL,
	[Account] [nvarchar](250) NULL,
	[ContraAct] [nvarchar](250) NULL,
	[LineMemo] [nvarchar](max) NULL,
	[Debit] [numeric](18, 0) NULL,
	[Credit] [numeric](18, 0) NULL,
	[AcctName] [nvarchar](250) NULL,
	[U_InvNo] [nvarchar](250) NULL,
	[KhoanMuc] [nvarchar](250) NULL,
	[NhanHang] [nvarchar](250) NULL,
	[NhomHang] [nvarchar](250) NULL,
	[DoiTac] [nvarchar](250) NULL,
	[NhanVien] [nvarchar](250) NULL,
	[LocationName] [nvarchar](250) NULL,
	[Ref3Line] [nvarchar](250) NULL,
	[Remarks] [nvarchar](max) NULL,
	[Ref2] [nvarchar](250) NULL,
	[U_S1No] [nvarchar](250) NULL,
	[U_VoucherNo] [nvarchar](250) NULL,
	[SoCT] [nvarchar](250) NULL,
	[FCDebit] [numeric](18, 0) NULL,
	[FCCredit] [numeric](18, 0) NULL,
	[FCCurrency] [nvarchar](250) NULL,
	[U_NAME] [nvarchar](250) NULL,
	[SystemDate] [datetime] NULL,
	[TaxDate] [datetime] NULL,
	[U_AdjTran] [nvarchar](250) NULL,
	[VatGroup] [nvarchar](250) NULL,
	[BaseSum] [nvarchar](250) NULL,
	[U_BPCode] [nvarchar](250) NULL,
	[U_BPName] [nvarchar](250) NULL,
	[U_TaxCode] [nvarchar](250) NULL,
	[U_StatusID] [nvarchar](250) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

/****** Object:  Table [dbo].[KQHDKD]    Script Date: 11/01/2024 12:01:43 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[KQHDKD](
	[Period] [nvarchar](15) NULL,
	[Account] [nvarchar](15) NULL,
	[CatName] [nvarchar](1000) NULL,
	[N0] [nvarchar](15) NULL,
	[N1] [nvarchar](15) NULL,
	[FrgnName] [nvarchar](1000) NULL,
	[CurPeriod] [numeric](19, 6) NULL,
	[PrvPeriod] [numeric](19, 6) NULL,
	[PrvPeriodYear] [numeric](19, 6) NULL,
	[CurPeriodVal] [numeric](19, 6) NULL,
	[PrvPeriodVal] [numeric](19, 6) NULL,
	[PrvPeriodYearVal] [numeric](19, 6) NULL,
	[CatId] [int] NULL,
	[Visible] [bit] NULL,
	[AdjTranStr] [nvarchar](1000) NULL,
	[PrintHeadr] [nvarchar](1000) NULL,
	[CompnyAddr] [nvarchar](1000) NULL,
	[AdjTran] [nvarchar](1000) NULL
) ON [PRIMARY]

GO

/****** Object:  Table [dbo].[LCTT]    Script Date: 11/01/2024 12:01:44 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[LCTT](
	[PeriodS] [datetime] NULL,
	[PeriodE] [datetime] NULL,
	[PrintHeadr] [nvarchar](250) NULL,
	[CompnyAddr] [nvarchar](250) NULL,
	[IsItalic] [int] NULL,
	[IsBold] [int] NULL,
	[OrderNo] [nvarchar](50) NULL,
	[CFItem] [nvarchar](100) NULL,
	[TemplateID] [int] NULL,
	[CatID] [int] NULL,
	[VisOrder] [int] NULL,
	[Father] [int] NULL,
	[IndentChar] [nvarchar](50) NULL,
	[Viet_Category] [nvarchar](250) NULL,
	[Eng_Category] [nvarchar](250) NULL,
	[Code] [nvarchar](50) NULL,
	[Remarks] [nvarchar](50) NULL,
	[PrePeriod] [decimal](19, 6) NULL,
	[InPeriod] [decimal](19, 6) NULL,
	[Para_1] [int] NULL,
	[Para_2] [int] NULL,
	[Para_3] [int] NULL,
	[Para_4] [int] NULL,
	[Para_5] [int] NULL,
	[OP_1] [int] NULL,
	[OP_2] [int] NULL,
	[OP_3] [int] NULL,
	[OP_4] [int] NULL,
	[OP_5] [int] NULL,
	[NguoiLapBC] [nvarchar](250) NULL,
	[KeToanTruong] [nvarchar](250) NULL,
	[GiamDoc] [nvarchar](250) NULL,
	[NgayIn] [datetime] NULL,
	[Levels] [int] NULL,
	[IsLink] [char](1) NULL
) ON [PRIMARY]

GO