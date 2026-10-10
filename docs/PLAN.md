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
Browser ── POST /api/orders ──► Order Service
  ▲                                  │
  │                                  ▼
  │                    order_db.orders → binlog → Debezium
  │                                  │
  │                                  ▼
  │                    order_cdc.order_db.orders
  │                       ├──► Inventory consumer (inventory-service)
  │                       │        │
  │                       │        ▼
  │                       │   inventory_db.reservations → binlog → Debezium
  │                       │                                  │
  │                       │                                  ▼
  │                       │                inventory_cdc.inventory_db.reservations
  │                       │                                  │
  │                       │                                  ▼
  │                       │                Notification consumer → notification_db
  │                       │                                  │
  │                       │                                  └── SSE /api/notifications/stream ──► Browser
  │                       │
  │                       └──► Flow observer (separate consumer group)
  │                                  │
  └──────────── SSE /api/flow/stream ◄┘
```

- `order-service`: `POST /api/orders`, phục vụ UI; host port được Docker Compose cấp động để có thể chạy nhiều replica.
- The UI offers demo identities `USER_A` and `USER_B`; `customer_id` is saved with the order and propagated through reservation/notification CDC so each UI filters its own history. This is a demo identity only, without authentication. The Kafka key remains the order primary key `id`.
- `inventory-service`: consumer của `order_cdc.order_db.orders`, chạy với consumer group `inventory-service`.
- `notification-service`: consumes `inventory_cdc.inventory_db.reservations`; exposes `GET /api/notifications?customerId=USER_A`, `GET /api/notifications/stream?customerId=USER_A`, and `GET /api/flow/stream?customerId=USER_A`.
- Flow observer của Notification Service đọc `order_cdc.order_db.orders` bằng consumer group riêng `notification-flow-observer`. Đây là nhánh quan sát để cập nhật giao diện; nó không dùng group `inventory-service` nên không lấy message khỏi Inventory consumer.
- Flow stream phát milestone kèm topic, partition và offset. UI đánh dấu Inventory đã xử lý khi reservation CDC xuất hiện; nó không giả định rằng chỉ vì order topic có event thì Inventory đã nhận xong.
- Flow trace giữ tối đa 200 event gần nhất trong bộ nhớ của Notification Service để gửi snapshot khi trình duyệt kết nối lại. Đây là telemetry cho demo, không phải lịch sử bền vững; restart Notification Service sẽ xóa snapshot này.
- Mỗi service chỉ ghi vào database của nó. Inventory DB và Order DB là hai MariaDB instance riêng; Notification DB là instance thứ ba.
- Kafka chạy một broker, order CDC topic có 3 partition để thử tối đa 3 Inventory consumer cùng lúc; replication factor vẫn là 1. Reservation CDC topic giữ 1 partition vì Notification hiện có một consumer.
- Order Service replicas cùng ghi vào Order DB; Debezium MariaDB connector vẫn là một task đọc binlog. Nhiều Order Service pod không đồng nghĩa có nhiều Kafka producer task.
- Order Service publishes container port 8080 on `0.0.0.0` with a dynamic host port for Tailnet access; Inventory Service remains on loopback. Notification Service publishes `8090` on `0.0.0.0` so remote UI clients can receive flow and notification SSE. Use `docker compose port --index <n> order-service 8080` to find each replica.

## Xử lý dữ liệu

- Order CDC key stays `{"id":"<order-id>"}`; `customer_id` travels in the value and does not become the partition key. The `__op` header carries the operation. Inventory processes `c` (insert) and `r` (snapshot), and ignores order updates/deletes in this demo.
- Legacy CDC records without `customer_id` are interpreted as `USER_A`, matching the default assigned by the database migrations.
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

Chạy nhiều Order Service và Inventory consumer cho bài tập scale:

```powershell
docker compose -f infra/docker-compose.yaml up -d --scale order-service=2 --scale inventory-service=3
docker compose -f infra/docker-compose.yaml ps
docker compose -f infra/docker-compose.yaml port --index 1 order-service 8080
docker compose -f infra/docker-compose.yaml port --index 2 order-service 8080
docker compose -f infra/docker-compose.yaml exec -T kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group inventory-service --members --verbose
```

Mở UI qua một trong các host port được Docker hiển thị. Gửi request tới từng Order Service port để quan sát các replica cùng ghi vào DB và Debezium phát event; ba Inventory consumer dùng chung group và chia nhau ba partition. Giao diện nhận milestone từ hai luồng SSE: order CDC được flow observer quan sát, còn kết quả cuối chỉ xuất hiện sau khi Notification lưu DB.
Tailnet access: run `tailscale ip -4` on the Docker host, then open the two Order Service replica URLs in separate tabs. Select `User A` in one tab and `User B` in the other to get separate notification and flow views while both use the shared inventory.

```powershell
docker compose -f infra/docker-compose.yaml logs -f inventory-service notification-service debezium-connect
```

Gọi API trực tiếp cần giữ nguyên key và body khi retry cùng một order:

```powershell
$idempotencyKey = [guid]::NewGuid().ToString()
$orderBody = '{"customerId":"USER_A","sku":"IPHONE-15","quantity":1}'
$orderPort = (docker compose -f infra/docker-compose.yaml port --index 1 order-service 8080).Split(':')[-1]
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:$orderPort/api/orders" `
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
curl.exe "http://localhost:8090/api/notifications?customerId=USER_A"
```

Database and Kafka/Connect ports stay on loopback: Kafka `9092`, Kafka Connect `8083`, Order DB `3307`, Inventory DB `3308`, Notification DB `3309`. Order Service and Notification Service bind `0.0.0.0` for Tailnet testing; the host firewall controls which machines can connect.

`order-db-init`, `inventory-db-init` and `notification-db-init` apply idempotent SQL migrations on existing named volumes. Existing rows without an identity are assigned to `USER_A`; the migrations preserve stored orders, reservations and notifications. `docker compose down` keeps data; `docker compose down -v` removes demo data, Kafka offsets and Debezium schema history.

## Cấu trúc chính

```text
infra/
  connectors/{order,inventory}-connector.json
  mariadb/{order,inventory,notification}/{init,migrate}.sql
  mariadb/notification/migrate.sql
  docker-compose.yaml
src/
  backend/{order-services,inventory-service,notification-service}/
  frontend/order-ui/{index.html,css/app.css,js/app.js}
```
