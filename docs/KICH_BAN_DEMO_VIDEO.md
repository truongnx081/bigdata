# Kịch bản demo luồng video

## 1. Chuẩn bị

Mở PowerShell riêng cho video và chạy:

```powershell
.\scripts\start-video.ps1
```

Mở `http://localhost:3000`. Tại mục **Chọn nguồn dữ liệu video**, dán URL YouTube hoặc chọn file video trên máy, bật phát lại nếu cần rồi bấm chạy.

## 2. Giới thiệu dữ liệu đầu vào

Chỉ vào khung video và nhóm thông tin nguồn:

> Mỗi giây, hệ thống trích xuất tốc độ ước lượng, vị trí, thời gian, mật độ, số phương tiện và tỷ lệ chiếm dụng. Sự kiện được gửi vào topic `traffic.video.raw` của Apache Kafka.

## 3. Giới thiệu xử lý realtime

Chỉ vào bốn trạng thái trong sơ đồ luồng:

> Spark Structured Streaming đọc topic video theo micro-batch hai giây. Cùng một pipeline có thể xử lý song song nguồn IoT và video nhưng kết quả được trả về hai topic riêng để không trộn dashboard.

## 4. Giới thiệu mô hình AI

Chỉ vào thẻ **Kết quả Isolation Forest**:

> Isolation Forest đánh giá điểm bất thường từ tốc độ, mật độ, số xe và tỷ lệ chiếm dụng. Hệ thống kết hợp điểm này với ngưỡng giao thông để phân loại ổn định, mật độ tăng, đông xe hoặc nguy cơ ùn tắc nghiêm trọng.

## 5. Giới thiệu output

Chỉ vào bốn KPI và biểu đồ:

- Tốc độ trung bình và mật độ cập nhật theo sự kiện video.
- Số phương tiện được nhận diện từ vùng chuyển động.
- Điểm rủi ro và cảnh báo lấy từ kết quả Spark, không tính ở frontend.
- Biểu đồ giữ 32 chu kỳ gần nhất để thấy xu hướng tăng/giảm.

## 6. Lưu ý khi trình bày

- Với file video, bật **Tự phát lại khi video kết thúc** trên dashboard.
- Dùng camera cố định để background subtraction ổn định.
- Nếu tốc độ ước lượng lệch nhiều, hiệu chỉnh `VIDEO_PIXELS_PER_METER` trong `.env`.
- Chờ Spark khoảng vài giây ở lần chạy đầu trước khi kết quả Isolation Forest xuất hiện.
