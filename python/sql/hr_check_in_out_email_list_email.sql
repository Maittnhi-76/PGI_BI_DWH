SELECT DISTINCT
    machamcong,
    tenchamcong,
    tennhanvien,
    emailcongty
FROM `pgibidwh.Human_Resources.dm_staff_info`
WHERE 1 = 1
    AND emailcongty is not null and emailcongty<>""
    -- AND tenchamcong IN ('thienvo')