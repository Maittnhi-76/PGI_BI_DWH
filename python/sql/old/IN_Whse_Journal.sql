Select  VoucherTypeName,DocEntry,S1No,VoucherNo,DocDate,t0.CreateDate,ItemCode,Dscription,InQty,OutQty,Warehouse,CalcPrice [Item Cost],TransValue,InvntAct,
case    when (CostAct+TrnsfrAct+PriceDifAc+VarianceAc+ExcRateAct+ClearAct+WipAct='' and t0.ObjType='67') then InvntAct 
        when (CostAct+TrnsfrAct+PriceDifAc+VarianceAc+ExcRateAct+ClearAct+WipAct='' and t0.ObjType<>'67') then CardCode 
        else CostAct+TrnsfrAct+PriceDifAc+VarianceAc+ExcRateAct+ClearAct+WipAct end DiffAcc,CardCode,
REPLACE(REPLACE(REPLACE(Comments, CHAR(9), ''), CHAR(10), ''),CHAR(13), '')  Comments ,Price [Unit Price],DocLineTotal,T1.U_NAME,dbo.trans(t0.ObjType) ObjType,
REPLACE(REPLACE(REPLACE(JrnlMemo, CHAR(9), ''), CHAR(10), ''),CHAR(13), '') JrnlMemo--,T0.*
from  S1_OINM t0 (nolock)
inner join OUSR T1  (nolock) on T0.UserSign=t1.USERID
where DocDate between '2024-10-01' and  '2024-12-31'