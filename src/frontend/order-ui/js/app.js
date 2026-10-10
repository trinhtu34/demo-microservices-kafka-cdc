const form = document.querySelector("#order-form");
const submitButton = document.querySelector("#submit-button");
const submitLabel = document.querySelector(".submit-button__label");
const resultPanel = document.querySelector("#result-panel");
const resultEyebrow = document.querySelector("#result-eyebrow");
const resultTitle = document.querySelector("#result-title");
const resultMessage = document.querySelector("#result-message");
const orderIdPanel = document.querySelector("#order-id-panel");
const orderId = document.querySelector("#order-id");
const customerInput = document.querySelector("#customer-id");
const skuInput = document.querySelector("#sku");
const quantityInput = document.querySelector("#quantity");
const notificationConnection = document.querySelector("#notification-connection");
const notificationConnectionLabel = document.querySelector("#notification-connection-label");
const notificationList = document.querySelector("#notification-list");
const notificationEmpty = document.querySelector("#notification-empty");
const flowConnection = document.querySelector("#flow-connection");
const flowConnectionLabel = document.querySelector("#flow-connection-label");
const flowRuntimeStatus = document.querySelector("#flow-runtime-status");
const flowOrderDetails = document.querySelector("#flow-order-details");
const flowOrderId = document.querySelector("#flow-order-id");
const flowEvents = document.querySelector("#flow-events");
const flowEventsEmpty = document.querySelector("#flow-events-empty");
const flowStepNames = ["order", "order-database", "order-cdc", "inventory", "reservation-cdc", "notification"];
const flowSteps = new Map([...document.querySelectorAll("[data-flow-step]")]
    .map((step) => [step.dataset.flowStep, step]));
const notificationsById = new Map();
const pendingOrderStorageKey = "order-cdc-demo.pending-order";
let pendingOrderAttempt = readPendingOrderAttempt();
let notificationEventSource = null;
let flowEventSource = null;
let activeFlowOrderId = null;
let flowProgressIndex = -1;
const seenFlowEvents = new Set();

if (pendingOrderAttempt) {
    customerInput.value = pendingOrderAttempt.customerId;
    customerInput.disabled = true;
    skuInput.value = pendingOrderAttempt.sku;
    quantityInput.value = String(pendingOrderAttempt.quantity);
    beginOrderFlow(pendingOrderAttempt.idempotencyKey);
    flowRuntimeStatus.textContent = "Request trước chưa nhận được phản hồi. Đang tiếp tục quan sát CDC cho order này.";
}

function readPendingOrderAttempt() {
    try {
        const saved = JSON.parse(sessionStorage.getItem(pendingOrderStorageKey));
        if (saved
            && typeof saved.idempotencyKey === "string"
            && typeof saved.sku === "string"
            && Number.isInteger(saved.quantity)
            && saved.quantity > 0) {
            const customerId = saved.customerId ?? "USER_A";
            if (customerId === "USER_A" || customerId === "USER_B") {
                return { ...saved, customerId };
            }
        }
    } catch {
        // Keep the form usable if session storage is unavailable.
    }
    return null;
}

function savePendingOrderAttempt(attempt) {
    pendingOrderAttempt = attempt;
    customerInput.disabled = true;
    try {
        sessionStorage.setItem(pendingOrderStorageKey, JSON.stringify(attempt));
    } catch {
        // The in-memory key still supports retries until this page closes.
    }
}

function clearPendingOrderAttempt() {
    pendingOrderAttempt = null;
    customerInput.disabled = false;
    try {
        sessionStorage.removeItem(pendingOrderStorageKey);
    } catch {
        // No persistent value was available to remove.
    }
}

function createIdempotencyKey() {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
        return window.crypto.randomUUID();
    }
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (character) => {
        const random = Math.floor(Math.random() * 16);
        const value = character === "x" ? random : (random & 0x3) | 0x8;
        return value.toString(16);
    });
}

function getPendingOrderAttempt(request) {
    if (pendingOrderAttempt) {
        const samePayload = pendingOrderAttempt.sku === request.sku
            && pendingOrderAttempt.customerId === request.customerId
            && pendingOrderAttempt.quantity === request.quantity;
        return samePayload ? pendingOrderAttempt : null;
    }

    const attempt = {
        idempotencyKey: createIdempotencyKey(),
        customerId: request.customerId,
        sku: request.sku,
        quantity: request.quantity,
    };
    savePendingOrderAttempt(attempt);
    return attempt;
}

function showResult({ state, eyebrow, title, message, id }) {
    resultPanel.hidden = false;
    resultPanel.dataset.state = state;
    resultPanel.setAttribute("aria-live", state === "error" ? "assertive" : "polite");
    resultEyebrow.textContent = eyebrow;
    resultTitle.textContent = title;
    resultMessage.textContent = message;
    orderIdPanel.hidden = !id;
    orderId.textContent = id ?? "";
}

function apiErrorMessage(body) {
    const details = body && typeof body.details === "object" && body.details !== null
        ? Object.entries(body.details).map(([field, message]) => {
            const label = field === "sku" ? "SKU" : field === "quantity" ? "Số lượng" : field;
            return `${label}: ${message}`;
        })
        : [];
    const message = body && typeof body.message === "string"
        ? body.message
        : "Order Service không thể tạo order.";

    return [message, ...details].join(" · ");
}

function setNotificationConnection(state, message) {
    notificationConnection.dataset.state = state;
    notificationConnectionLabel.textContent = message;
}

function setFlowConnection(state, message) {
    flowConnection.dataset.state = state;
    flowConnectionLabel.textContent = message;
}

function renderFlowSteps() {
    for (const [index, name] of flowStepNames.entries()) {
        const step = flowSteps.get(name);
        if (!step) {
            continue;
        }

        let state = "waiting";
        let label = "Đang chờ";
        if (!activeFlowOrderId) {
            state = "idle";
            label = "Chưa bắt đầu";
        } else if (index <= flowProgressIndex) {
            state = "complete";
            label = "Đã quan sát";
        } else if (index === flowProgressIndex + 1) {
            state = "active";
            label = index === 0 ? "Đang gửi order" : "Đang chờ dữ liệu";
        }

        step.dataset.state = state;
        step.querySelector(".flow__step-status").textContent = label;
    }
}

function beginOrderFlow(orderId) {
    activeFlowOrderId = orderId;
    flowProgressIndex = -1;
    seenFlowEvents.clear();
    flowEvents.replaceChildren();
    flowEventsEmpty.hidden = false;
    flowOrderDetails.hidden = false;
    flowOrderId.textContent = orderId;
    flowRuntimeStatus.textContent = "Đang gửi order; các mốc tiếp theo sẽ được đánh dấu khi quan sát thấy CDC event.";
    renderFlowSteps();
}

function addFlowEvent(key, titleText, detailText, timestamp, rank) {
    if (seenFlowEvents.has(key)) {
        return;
    }
    seenFlowEvents.add(key);

    const item = document.createElement("li");
    item.className = "flow__event";
    item.dataset.rank = String(rank);

    const title = document.createElement("p");
    title.className = "flow__event-title";
    title.textContent = `${titleText} · ${flowTime(timestamp)}`;

    const detail = document.createElement("p");
    detail.className = "flow__event-detail";
    detail.textContent = detailText;

    item.append(title, detail);
    const nextEvent = [...flowEvents.children].find((eventItem) => Number(eventItem.dataset.rank) > rank);
    flowEvents.insertBefore(item, nextEvent ?? null);
    while (flowEvents.children.length > 8) {
        flowEvents.firstElementChild.remove();
    }
    flowEventsEmpty.hidden = flowEvents.children.length > 0;
}

function flowTime(value) {
    const date = value ? new Date(value) : new Date();
    return Number.isNaN(date.getTime()) ? new Date().toLocaleTimeString("vi-VN") : date.toLocaleTimeString("vi-VN");
}

function advanceFlow(completedIndex, message) {
    const previousProgressIndex = flowProgressIndex;
    flowProgressIndex = Math.max(flowProgressIndex, completedIndex);
    if (completedIndex >= previousProgressIndex) {
        flowRuntimeStatus.textContent = message;
    }
    renderFlowSteps();
}

function handleFlowProgress(progress) {
    if (!progress
        || progress.customerId !== customerInput.value
        || progress.orderId !== activeFlowOrderId
        || typeof progress.milestone !== "string") {
        return;
    }

    const detail = `${progress.topic} · partition ${progress.partition} · offset ${progress.offset}`;
    const eventKey = `${progress.milestone}:${progress.topic}:${progress.partition}:${progress.offset}`;
    if (progress.milestone === "ORDER_CDC_CAPTURED") {
        addFlowEvent(eventKey, "Debezium event đã tới Kafka", detail, progress.observedAt, 2);
        advanceFlow(2, "Đã quan sát order CDC event trong Kafka. Inventory đang xử lý order bất đồng bộ.");
    } else if (progress.milestone === "RESERVATION_CDC_CAPTURED") {
        addFlowEvent(eventKey, "Inventory result đã tới Kafka", detail, progress.observedAt, 4);
        advanceFlow(4, "Đã quan sát reservation CDC event. Notification đang lưu kết quả.");
    }
}

function markOrderSaved(orderId, httpStatus) {
    if (orderId !== activeFlowOrderId) {
        beginOrderFlow(orderId);
    }
    addFlowEvent("order-saved", "Order Service xác nhận đã lưu", `POST /api/orders · HTTP ${httpStatus}`, new Date(), 1);
    advanceFlow(1, "Order đã commit vào MariaDB. Đang chờ Debezium phát hiện thay đổi trong binlog.");
}

function markNotificationDelivered(notification) {
    if (!notification || notification.orderId !== activeFlowOrderId) {
        return;
    }
    addFlowEvent("notification-delivered", "Notification đã lưu và gửi SSE", `${notification.status} · ${notification.sku} × ${notification.quantity}`, notification.createdAt, 5);
    advanceFlow(5, "Hoàn tất: order đã đi qua CDC, Inventory và Notification tới giao diện.");
}

function rejectOrderFlow(message) {
    activeFlowOrderId = null;
    flowRuntimeStatus.textContent = message;
    renderFlowSteps();
    const orderStep = flowSteps.get("order");
    if (orderStep) {
        orderStep.dataset.state = "error";
        orderStep.querySelector(".flow__step-status").textContent = "Request bị từ chối";
    }
}

function notificationStatus(status) {
    return status === "RESERVED" ? "Đã giữ hàng" : "Không giữ được hàng";
}

function notificationReason(reason) {
    if (reason === "INSUFFICIENT_STOCK") {
        return "Tồn kho không đủ cho số lượng yêu cầu.";
    }
    if (reason === "SKU_NOT_FOUND") {
        return "Không tìm thấy SKU trong inventory.";
    }
    return reason || "";
}

function notificationTime(value) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? "" : date.toLocaleString("vi-VN");
}

function createNotificationItem(notification) {
    const item = document.createElement("li");
    item.className = "notification-item";
    item.dataset.state = notification.status;

    const heading = document.createElement("div");
    heading.className = "notification-item__heading";

    const title = document.createElement("h3");
    title.textContent = `${notification.sku} · ${notification.quantity} sản phẩm`;

    const status = document.createElement("span");
    status.className = "notification-item__status";
    status.textContent = notificationStatus(notification.status);
    heading.append(title, status);

    const order = document.createElement("code");
    order.className = "notification-item__order";
    order.textContent = `${notification.customerId} · ORDER ${notification.orderId}`;

    item.append(heading, order);

    const reason = notificationReason(notification.reason);
    if (reason) {
        const reasonText = document.createElement("p");
        reasonText.className = "notification-item__reason";
        reasonText.textContent = reason;
        item.append(reasonText);
    }

    const time = notificationTime(notification.createdAt);
    if (time) {
        const timestamp = document.createElement("p");
        timestamp.className = "notification-item__time";
        timestamp.textContent = time;
        item.append(timestamp);
    }

    return item;
}

function mergeNotifications(items) {
    for (const item of items) {
        if (item
            && item.id !== undefined
            && item.customerId === customerInput.value
            && typeof item.orderId === "string") {
            notificationsById.set(String(item.id), item);
            markNotificationDelivered(item);
        }
    }

    const newest = [...notificationsById.values()]
        .sort((left, right) => Number(right.id) - Number(left.id))
        .slice(0, 20);
    notificationsById.clear();
    for (const item of newest) {
        notificationsById.set(String(item.id), item);
    }

    notificationList.replaceChildren(...newest.map(createNotificationItem));
    notificationEmpty.hidden = newest.length > 0;
}

function parseEventData(event) {
    try {
        return JSON.parse(event.data);
    } catch {
        setNotificationConnection("error", "Dữ liệu notification không hợp lệ");
        return null;
    }
}

function connectNotificationStream() {
    notificationEventSource?.close();
    if (window.location.protocol === "file:") {
        setNotificationConnection("error", "Mở giao diện qua cổng được publish bởi Docker Compose");
        return;
    }

    const customerId = encodeURIComponent(customerInput.value);
    const streamUrl = `${window.location.protocol}//${window.location.hostname}:8090/api/notifications/stream?customerId=${customerId}`;
    notificationEventSource = new EventSource(streamUrl);

    notificationEventSource.onopen = () => setNotificationConnection("connected", "Đang nhận realtime");
    notificationEventSource.onerror = () => setNotificationConnection("error", "Mất kết nối · đang thử lại");
    notificationEventSource.addEventListener("snapshot", (event) => {
        const snapshot = parseEventData(event);
        if (Array.isArray(snapshot)) {
            mergeNotifications(snapshot);
        }
    });
    notificationEventSource.addEventListener("notification", (event) => {
        const notification = parseEventData(event);
        if (notification) {
            mergeNotifications([notification]);
        }
    });
}

function connectFlowStream() {
    flowEventSource?.close();
    if (window.location.protocol === "file:") {
        setFlowConnection("error", "Mở giao diện qua cổng được publish bởi Docker Compose");
        return;
    }

    const customerId = encodeURIComponent(customerInput.value);
    const streamUrl = `${window.location.protocol}//${window.location.hostname}:8090/api/flow/stream?customerId=${customerId}`;
    flowEventSource = new EventSource(streamUrl);
    flowEventSource.onopen = () => setFlowConnection("connected", "Flow stream đang hoạt động");
    flowEventSource.onerror = () => setFlowConnection("error", "Mất flow stream · đang thử lại");
    flowEventSource.addEventListener("snapshot", (event) => {
        try {
            const snapshot = JSON.parse(event.data);
            if (Array.isArray(snapshot)) {
                snapshot.forEach(handleFlowProgress);
            }
        } catch {
            setFlowConnection("error", "Flow snapshot không hợp lệ");
        }
    });
    flowEventSource.addEventListener("flow", (event) => {
        try {
            handleFlowProgress(JSON.parse(event.data));
        } catch {
            setFlowConnection("error", "Flow event không hợp lệ");
        }
    });
}

form.addEventListener("submit", async (event) => {
    event.preventDefault();

    if (window.location.protocol === "file:") {
        showResult({
            state: "error",
            eyebrow: "TRANG ĐANG MỞ TỪ FILE",
            title: "Mở giao diện qua Order Service",
            message: "File local chỉ hiển thị giao diện. Hãy mở UI qua host port được Docker Compose publish.",
        });
        return;
    }

    if (!form.reportValidity()) {
        return;
    }

    const formData = new FormData(form);
    const request = {
        customerId: String(formData.get("customerId")),
        sku: String(formData.get("sku")).trim(),
        quantity: Number(formData.get("quantity")),
    };
    const attempt = getPendingOrderAttempt(request);
    if (!attempt) {
        showResult({
            state: "error",
            eyebrow: "CÒN REQUEST CHƯA XÁC NHẬN",
            title: "Gửi lại order đang chờ trước",
            message: "Giữ nguyên user, SKU và số lượng để retry cùng Idempotency-Key. Sau khi nhận kết quả, bạn có thể tạo order mới.",
        });
        return;
    }

    submitButton.disabled = true;
    submitLabel.textContent = "Đang lưu order…";
    form.setAttribute("aria-busy", "true");
    beginOrderFlow(attempt.idempotencyKey);
    showResult({
        state: "loading",
        eyebrow: "ORDER SERVICE",
        title: "Đang lưu order",
        message: "Đang chờ database commit.",
    });

    try {
        const response = await fetch("/api/orders", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Idempotency-Key": attempt.idempotencyKey,
            },
            body: JSON.stringify(request),
        });
        const body = await response.json().catch(() => null);

        if (!response.ok) {
            if (response.status < 500) {
                clearPendingOrderAttempt();
                rejectOrderFlow("Order Service từ chối request; không có CDC milestone mới được xác nhận.");
            } else {
                flowRuntimeStatus.textContent = "Order Service chưa xác nhận được kết quả. Tiếp tục theo dõi CDC theo order ID để phát hiện trường hợp commit nhưng mất phản hồi.";
            }
            showResult({
                state: "error",
                eyebrow: `HTTP ${response.status}`,
                title: "Chưa tạo được order",
                message: apiErrorMessage(body),
            });
            return;
        }

        if (!body || typeof body.id !== "string") {
            throw new Error("Order Service trả về response không đúng định dạng.");
        }

        clearPendingOrderAttempt();
        markOrderSaved(body.id, response.status);
        showResult({
            state: "success",
            eyebrow: `HTTP ${response.status} · ORDER ĐÃ LƯU`,
            title: response.status === 201 ? "Order đã được tạo" : "Order này đã được ghi nhận trước đó",
            message: response.status === 201
                ? "Inventory sẽ giữ hàng bất đồng bộ. Kết quả được lưu qua CDC và hiện ở mục Kết quả giữ hàng."
                : "Request dùng cùng Idempotency-Key nên Order Service trả lại order đã tạo trước đó.",
            id: body.id,
        });
    } catch (error) {
        flowRuntimeStatus.textContent = "Chưa nhận được HTTP response. Luồng CDC vẫn đang được theo dõi theo order ID này.";
        showResult({
            state: "error",
            eyebrow: "KHÔNG KẾT NỐI ĐƯỢC",
            title: "Chưa nhận được phản hồi",
            message: error instanceof TypeError
                ? "Không kết nối được Order Service. Hãy kiểm tra Docker Compose và tải lại trang."
                : error.message,
        });
    } finally {
        submitButton.disabled = false;
        submitLabel.textContent = "Tạo order";
        form.removeAttribute("aria-busy");
    }
});

customerInput.addEventListener("change", () => {
    notificationsById.clear();
    notificationList.replaceChildren();
    notificationEmpty.hidden = false;
    activeFlowOrderId = null;
    flowProgressIndex = -1;
    seenFlowEvents.clear();
    flowEvents.replaceChildren();
    flowEventsEmpty.hidden = false;
    flowOrderDetails.hidden = true;
    resultPanel.hidden = true;
    flowRuntimeStatus.textContent = "Chọn user rồi tạo order để theo dõi luồng CDC riêng.";
    renderFlowSteps();
    connectNotificationStream();
    connectFlowStream();
});

if (pendingOrderAttempt) {
    showResult({
        state: "pending",
        eyebrow: "REQUEST ĐANG CHỜ XÁC NHẬN",
        title: "Có thể retry cùng một order",
        message: "Bấm Tạo order để gửi lại cùng Idempotency-Key và nhận kết quả trước đó nếu order đã được lưu.",
    });
}

connectNotificationStream();
connectFlowStream();
