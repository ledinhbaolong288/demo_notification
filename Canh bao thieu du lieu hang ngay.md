# Chức năng gửi email cảnh báo thiếu dữ liệu

## Đang có
Hàng ngày, hệ thống đã có job tự kiểm tra việc có hay không có dữ liệu trong các table cần theo dõi và lưu lại kết quả tại table TBL_DATA_CHECK.
Note: thông tin về những table cần kiểm tra được lưu tại META_TABLE_NAMES

## Công việc của Job cảnh báo

### Bước 1
Kiểm tra TBL_DATA_CHECK có dữ liệu trong ngày
Chạy SQL
```SQL
select 1
from dual
where exists (
  select 1
  from tbl_data_check t
  where t.check_date >= trunc(sysdate)
)
;
```
1.1. Nếu SQL không có dữ liệu -> Gửi email cảnh báo "Chưa có dữ liệu kiểm tra table trong ngày YYYY-MM-DD" --> kết thúc
1.2. Nếu SQL có dữ liệu -> sang bước 2

### Bước 2
Nếu table TBL_DATA_CHECK phát sinh dữ liệu thì lấy dữ liệu ghi nhận lỗi như sau
```SQL
select t.*, m.data_non_exists_time
from tbl_data_check t
     join meta_table_names m on t.tbl_name = m.full_tbl_schema_name
where t.check_date >= trunc(sysdate)
      and t.STATUS != 'HAS_DATA'
      and not exists ( select 1 
                       from tbl_data_check t1 
                       where t1.check_date > t.check_date 
                             and t1.tbl_name = t.tbl_name)
;
```
Các các trường hợp xảy ra như sau:

2.1. Không có dữ liệu -> Thông báo "Dữ liệu trong ngày đã đủ - YYYY-MM-DD" -> kết thúc
2.2. Có dữ liệu báo lỗi -> Chuyển sang bước 3 để báo lỗi

### Bước 3:
Dữ liệu lấy được ở bước 2 sẽ có dạng như sau (CSV)
```CSV
"TBL_NAME","STATUS","CHECK_DATE","NOTE","DATA_NON_EXISTS_TIME"
"DAB2HDB.MV_FCC_CUSTOMER ","NO_DATA","20-APR-26 07.50.55.436823 AM","Fail (không có dữ liệu NGAY = T-1)","* * * * 0,1"
"DAB2HDB.VWDT_HUY_DONG_CKH_W_LSDCV ","NO_DATA","20-APR-26 07.50.56.400567 AM","Fail (không có dữ liệu NGAY = T-1)","* * * * 0,1"
"DAB2HDB.VWDT_HUY_DONG_CKH ","NO_DATA","20-APR-26 07.50.56.260840 AM","Fail (không có dữ liệu NGAY = T-1)","* * * * 0,1"
"DAB2HDB.VWDT_HUY_DONG_KKH ","NO_DATA","20-APR-26 07.50.56.395922 AM","Fail (không có dữ liệu NGAY = T-1)","* * * * 0,1"
"DAB2HDB.FCC_ACCOUNT_TRANSACTION_LISTING ","NO_DATA","20-APR-26 07.50.55.438976 AM","Fail (không có dữ liệu NGAY = T-1)","* * * * 0,1"
"DAB2HDB.TBL_HUY_DONG_CKH_W_LSDCV_DAILY ","NO_DATA","20-APR-26 07.50.56.399020 AM","Fail (không có dữ liệu NGAY = T-1)","* * * * 0,1"
"DAB2HDB.TBL_HUY_DONG_CKH_W_LSDCV ","NO_DATA","20-APR-26 07.50.56.400199 AM","Fail (không có dữ liệu NGAY = T-1)","* * * * 0,1"
```
Cần quan tâm các cột:
- TBL_NAME: Tên bảng
- CHECK_DATE: thời điểm kiểm tra (timestamp)
- DATA_NON_EXISTS_TIME: chuỗi Cron định nghĩa thời điểm không có số liệu

Kiểm tra CHECK_DATE có thuộc thời gian được định nghĩa bởi DATA_NON_EXISTS_TIME hay không và xác định MATCH hoặc UNMATCH. 
VD: nếu DATA_NON_EXISTS_TIME = '* * * * 0,1' (nghĩa là tất cả các giờ, phút trong ngày T7 và CN) và:
- CHECK_DATE là T7 hoặc CN -> MATCH
- CHECK_DATE là các thứ khác trong tuần -> UNMATCH

Với kết quả kiểm tra cho từng table:
- Nếu toàn bộ các dòng đều MATCH -> Thông báo "Dữ liệu trong ngày đã đủ - YYYY-MM-DD" -> kết thúc
- Nếu tồn tại dòng UNMATCH -> Gửi mail với tiêu đề "[DWH_ALERT] Thiếu dữ liệu trong ngày YYYY-MM-DD" và nội dung là danh sách các TBL_NAME của dòng UNMATCH
