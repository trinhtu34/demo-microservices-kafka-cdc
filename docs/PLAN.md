# Kafka CDC microservices demo

## Mục tiêu

Demo một luồng dữ liệu đơn giản, dễ quan sát:

1. Order Service ghi order vào MariaDB.
2. Debezium đọc binlog và phát thay đổi lên Kafka.
3. Inventory Service nhận event, cập nhật tồn kho và lưu kết quả giữ hàng vào database riêng.
4. Debezium bắt thay đổi reservation; Notification Service lưu kết quả vào database riêng và gửi cập nhật trực tiếp lên giao diện qua SSE.

Order Service không gửi Kafka message trực tiếp. Database commit là nguồn sự thật cho CDC. Phản hồi tạo order chỉ xác nhận order đã được ghi; kết quả giữ hàng đến sau theo luồng bất đồng bộ.

`POST /api/orders` yêu cầu header `Idempotency-Key` là UUID; UUID này cũng là `orders.id`. Cùng key và cùng payload trả lại order đã có (`200`); cùng key nhưng SKU/số lượng khác trả `409`. UI giữ request chưa xác nhận trong session để retry sau timeout dùng lại key cũ.

## Thành phần và luồng

```text
Browser
  │ POST /api/orders                         SSE /api/notifications/stream
  ▼                                                       ▲
Order Service → order_db.orders → binlog → Debezium      │
                                         │                │
                                         ▼                │
                         order_cdc.order_db.orders       │
                                         │                │
                                         ▼                │
Inventory Service → inventory_db.reservations → binlog   │
                                         │                │
                                         ▼                │
                  inventory_cdc.inventory_db.reservations│
                                         │                │
                                         ▼                │
               Notification Service → notification_db ───┘
```

- `order-service`: `POST /api/orders`, phục vụ UI tại `http://localhost:8081/`.
- `inventory-service`: consumer của `order_cdc.order_db.orders`, chạy với consumer group `inventory-service`.
- `notification-service`: consumer của `inventory_cdc.inventory_db.reservations`; cung cấp `GET /api/notifications` và `GET /api/notifications/stream`.
- Mỗi service chỉ ghi vào database của nó. Inventory DB và Order DB là hai MariaDB instance riêng; Notification DB là instance thứ ba.
- Kafka chạy một broker, topic demo có một partition và replication factor 1.

## Xử lý dữ liệu

- Order CDC có key `{"id":"<order-id>"}`; operation nằm trong Kafka header `__op`. Inventory xử lý `c` (insert) và `r` (snapshot), bỏ qua update/delete vì demo không sửa/xóa order.
- Inventory xác thực key/value, sau đó trong một transaction: chèn reservation `PROCESSING`, trừ stock bằng điều kiện `stock >= quantity`, rồi ghi trạng thái cuối `RESERVED` hoặc `REJECTED`. `PROCESSING` không được commit riêng.
- `reservations.order_id` là khóa duy nhất. Nếu event được phát lại sau khi DB commit, consumer xác nhận SKU/số lượng khớp rồi bỏ qua, nên stock không bị trừ lần hai. Cùng ID nhưng payload khác hoặc trạng thái cũ không hoàn tất được lưu nguyên key/value vào `inventory_db.cdc_errors`.
- Consumer chỉ xác nhận Kafka offset sau khi transaction reservation hoặc quarantine commit thành công.
- Nếu DB hoặc xử lý lưu trữ gặp lỗi ngoài dữ liệu CDC sai, listener retry mỗi giây vô thời hạn; record vẫn giữ offset chưa commit và partition chờ DB hoạt động lại.
- Debezium có thể phát event cho insert `PROCESSING` và update trạng thái cuối của reservation. Notification chỉ lưu và phát `RESERVED`/`REJECTED`; `PROCESSING` bị bỏ qua.
- Notification được lưu với `order_id` duy nhất trước khi SSE broadcast và Kafka offset được xác nhận. Nếu xử lý lại, service chỉ phát lại khi SKU, số lượng và kết quả khớp với row đã lưu; kết quả xung đột được quarantine. UI loại bản lặp theo notification ID.
- Event Notification thiếu/sai operation hoặc payload được lưu nguyên key/value theo topic/partition/offset vào `notification_db.notification_errors` trước khi ack.
- Nếu ghi Notification DB lỗi, listener retry mỗi giây vô thời hạn và chưa xác nhận offset.
- SSE gửi snapshot tối đa 20 notification khi trình duyệt kết nối, sau đó gửi notification mới. Nếu service tạm ngắt, trình duyệt tự kết nối lại và nhận snapshot mới.

## Dữ liệu demo

Inventory được seed ban đầu:

| SKU | Tồn kho |
| --- | ---: |
| `IPHONE-15` | 10 |
| `MACBOOK-M3` | 3 |
| `AIRPODS-PRO` | 0 |

Kết quả cuối:

- Có đủ hàng: `RESERVED`, stock giảm đúng số lượng.
- SKU không tồn tại hoặc tồn kho không đủ: `REJECTED`, stock giữ nguyên; notification hiển thị lý do.

## Chạy và quan sát

```powershell
docker compose -f infra/docker-compose.yaml up -d --build
docker compose -f infra/docker-compose.yaml ps
```

Mở `http://localhost:8081/`, tạo order, rồi theo dõi:

```powershell
docker compose -f infra/docker-compose.yaml logs -f inventory-service notification-service debezium-connect
```

Gọi API trực tiếp cần giữ nguyên key và body khi retry cùng một order:

```powershell
$idempotencyKey = [guid]::NewGuid().ToString()
$orderBody = '{"sku":"IPHONE-15","quantity":1}'
Invoke-RestMethod -Method Post -Uri http://localhost:8081/api/orders `
  -Headers @{ 'Idempotency-Key' = $idempotencyKey } `
  -ContentType 'application/json' -Body $orderBody
```

Đọc event CDC từ Kafka:

```powershell
docker compose -f infra/docker-compose.yaml exec -T kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic order_cdc.order_db.orders --from-beginning --property print.key=true --property print.headers=true
docker compose -f infra/docker-compose.yaml exec -T kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic inventory_cdc.inventory_db.reservations --from-beginning --property print.key=true --property print.headers=true
```

Kiểm tra notification đã được lưu:

```powershell
curl.exe http://localhost:8090/api/notifications
```

Các cổng host chỉ bind vào loopback: Order UI/API `8081`, Inventory DB `3308`, Kafka `9092`, Kafka Connect `8083`, Inventory Service `8082`, Notification DB `3309`, Notification Service `8090`.

`inventory-db-init` áp dụng lại SQL idempotent để cấp quyền CDC trên cả volume inventory đã tồn tại. `notification-db-init` áp dụng schema/user và migration idempotent cho volume notification. Các service DB dùng named volume; `docker compose down` giữ dữ liệu. `docker compose down -v` xóa dữ liệu, Kafka offsets và Debezium schema history để reset toàn bộ demo.

## Cấu trúc chính

```text
infra/
  connectors/{order,inventory}-connector.json
  mariadb/{order,inventory,notification}/init.sql
  mariadb/notification/migrate.sql
  docker-compose.yaml
src/
  backend/{order-services,inventory-service,notification-service}/
  frontend/order-ui/{index.html,css/app.css,js/app.js}
```
