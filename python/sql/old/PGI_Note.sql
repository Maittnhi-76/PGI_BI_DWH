
/*-- Kiểm tra chấm công trên Cloud và SQL bằng nhau*/
-- 1.Cloud
SELECT 
    tennhanvien,
    tenchamcong,
    ngaycham,
    MIN(checkin) AS checkin,
    MAX(checkin) AS checkin
FROM `pgi-dwh.human_resource.tb_dm_checkinout_detail` 
where ngaycham >= '2024-10-01' and ngaycham<'2024-10-21'
GROUP BY tennhanvien, tenchamcong, NGAYCHAM
ORDER BY ngaycham
-- 2.SQL
SELECT 
    t1.machamcong,
    tennhanvien,
    tenchamcong,
    ngaycham,
    MIN(GIOCHAM) AS checkin,
    CASE WHEN MAX(giocham) = MIN(giocham) THEN NULL ELSE MAX(giocham) END AS checkout
FROM CHECKINOUT T1
LEFT JOIN NHANVIEN T2 ON T1.MaChamCong = T2.MaChamCong 
where ngaycham>='2024-10-01' and ngaycham<'2024-10-21'
GROUP BY t1.machamcong, tennhanvien, tenchamcong, NGAYCHAM
ORDER BY ngaycham