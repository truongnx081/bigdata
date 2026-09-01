# Kịch bản demo IoT trong 5 phút

## 1. Mở hệ thống

Chạy `.\scripts\start-demo.ps1`, chờ các dịch vụ sẵn sàng rồi mở `http://localhost:8101`.

Giới thiệu ngắn:

> Hệ thống mô phỏng sáu cảm biến giao thông IoT. Mỗi giây, dữ liệu tốc độ, vị trí, thời gian, mật độ và số xe được gửi qua Kafka, xử lý bằng Spark Streaming và đánh giá bất thường bằng Isolation Forest.

## 2. Kịch bản bình thường

Chọn **Bình thường** và chỉ vào bảng dữ liệu theo tuyến:

- Mỗi chu kỳ có sáu sự kiện, mỗi tuyến một sự kiện.
- Tốc độ nhìn chung ổn định, mật độ thấp.
- Kết quả có dao động nhỏ để mô phỏng sai số cảm biến.

## 3. Kịch bản giờ cao điểm

Chọn **Giờ cao điểm**, chờ hai đến ba chu kỳ xử lý rồi quan sát:

- Mật độ của cả sáu tuyến tăng.
- Tốc độ trung bình giảm.
- Một số tuyến chuyển sang mức ùn tắc và xuất hiện cảnh báo.

## 4. Kịch bản trời mưa

Chọn **Trời mưa**:

- Tốc độ giảm so với điều kiện bình thường.
- Mật độ và tỷ lệ chiếm dụng tăng vừa phải.
- Trạng thái thường nằm giữa đông xe và ùn tắc tùy tuyến.

## 5. Kịch bản sự cố

Chọn **Sự cố 30 giây**:

- Chỉ một tuyến ngẫu nhiên bị tác động mạnh.
- Tốc độ tuyến đó giảm sâu, mật độ tiến gần 91%.
- Các tuyến còn lại tiếp tục gần trạng thái bình thường.
- Sau 30 giây, hệ thống tự trở về kịch bản trước đó.

Giải thích rằng Isolation Forest phát hiện điểm khác thường, còn các ngưỡng nghiệp vụ biến tốc độ và mật độ thành trạng thái dễ hiểu.

## 6. Kết luận

> Demo thể hiện đầy đủ một luồng Big Data thời gian thực: IoT tạo dữ liệu, Kafka tiếp nhận sự kiện, Spark xử lý liên tục, Isolation Forest phát hiện bất thường và web hiển thị mật độ, tốc độ trung bình cùng cảnh báo ùn tắc.

## Xử lý nhanh nếu demo chưa hiện kết quả

1. Chờ lần đầu Spark tải thư viện và huấn luyện mô hình.
2. Kiểm tra `docker compose ps` để chắc chắn Kafka, Spark và IoT đều đang chạy.
3. Tải lại `http://localhost:8101`.
4. Nếu đã chạy bản cũ trước đó, dùng `.\scripts\stop-demo.ps1` rồi chạy lại.
