# ОДИН Dockerfile НА ВСЕ СЕРВИСЫ МОНОРЕПОЗИТОРИЯ.
#
# Сервис выбирается аргументом сборки:
#   docker build --build-arg SERVICE=answers -t ghcr.io/ivanvnew75/answers:0.1.0 .
#
# Почему так, а не по Dockerfile на сервис: четыре почти одинаковых файла
# расходятся на первой же правке (обновили базовый образ в трёх из четырёх).
# Один файл + матрица в CI — это то же самое, но без дрейфа.
# Цена: контекст сборки — весь репозиторий, и правка любого сервиса
# инвалидирует кэш слоя `COPY . .` у всех. Для четырёх маленьких сервисов
# приемлемо; для тридцати понадобился бы bazel/nx с графом зависимостей.

# ---------- build ----------
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Сначала только go.mod/go.sum: слой с зависимостями не должен
# инвалидироваться от правки кода.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG SERVICE
ARG VERSION=dev
ARG COMMIT=unknown

# CGO_ENABLED=0 — статическая линковка, обязательна для distroless/static:
# там нет libc вообще. -trimpath убирает пути сборочной машины из бинарника
# (это и утечка, и помеха воспроизводимости).
RUN test -n "$SERVICE" || (echo "нужен --build-arg SERVICE=<имя>" && exit 1) && \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
      -o /out/app ./services/${SERVICE}

# ---------- runtime ----------
# Выбор базы измерен в курсе 12-factor-app (см. users/Dockerfile):
# scratch ломает HTTPS (нет корневых сертификатов) и таймзоны (нет tzdata),
# alpine на 35% больше. distroless/static:nonroot — компромисс.
FROM gcr.io/distroless/static-debian12:nonroot

# 65532 числом, а не именем nonroot: securityContext.runAsNonRoot
# в Kubernetes проверяет «не root» только по числовому UID.
USER 65532:65532

COPY --from=builder /out/app /usr/local/bin/app

EXPOSE 8080

# Форма exec: в shell-форме PID 1 стал бы /bin/sh, и SIGTERM от Kubernetes
# пришёл бы ему, а не приложению — graceful shutdown не сработал бы.
ENTRYPOINT ["/usr/local/bin/app"]
