with base_date as 
    (
    SELECT 
    t1.dates,
    t1.weekday_name,
    t2.machamcong,
    t2.tenchamcong,
    t2.tennhanvien  
    FROM `pgibidwh.Financials.dm_day_month_year` t1
    LEFT JOIN (SELECT DISTINCT machamcong, tenchamcong, tennhanvien 
                FROM `pgibidwh.Human_Resources.dm_staff_info`) t2    ON 1=1
    WHERE   date_trunc(cast(t1.dates as date),month) =date_trunc(current_date()-1,month) and cast(t1.dates as date)< current_date()
    ORDER BY 
    t1.dates ASC
    )


    select distinct
    t1.tennhanvien,
    t1.tenchamcong,
    t1.machamcong,
    t1.dates,
    case when t1.weekday_name = 'Monday' then 'Hai'
    when t1.weekday_name = 'Tuesday' then 'Ba'
    when t1.weekday_name = 'Wednesday' then 'Tư'
    when t1.weekday_name = 'Thursday' then 'Năm'
    when t1.weekday_name = 'Friday' then 'Sáu'
    when t1.weekday_name = 'Saturday' then 'Bảy'
    when t1.weekday_name = 'Sunday' then 'Chủ Nhật' end as day,
    FORMAT_DATETIME("%H:%M", checkin) checkin,
    FORMAT_DATETIME("%H:%M", t2.checkout) checkout,
    round(datetime_diff(t2.checkout, t2.checkin, minute)/60 -1,1) working_hour
    FROM base_date t1
    left join `pgibidwh.Human_Resources.checkin_checkout_detail` t2 on cast(t1.dates as date)=cast(t2.ngaycham as date) and cast(t1.machamcong as STRING)=cast(t2.machamcong as STRING)
    where  date_trunc(cast(t1.dates as date),month) =date_trunc(current_date()-1,month) and cast(t1.dates as date)< current_date()
    order by t1.dates