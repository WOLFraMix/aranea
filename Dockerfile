# syntax=docker/dockerfile:1

# ============================================================
# Этап 1: сборка. Полноценный тулчейн Go на Alpine.
# Сервис выбирается аргументом: --build-arg SERVICE=api|fetcher|indexer
# ============================================================
FROM golang:1.27.0-alpine AS build

ARG SERVICE=api
WORKDIR /src

# Сначала только манифесты зависимостей: этот слой кэшируется,
# и `go mod download` не выполняется заново, пока не изменились go.mod/go.sum
COPY go.mod go.sum ./
RUN go mod download

# Теперь код — при каждом изменении кода качать зависимости заново не придётся
COPY . .

# CGO_ENABLED=0 — статический бинарник без libc.
# Важно: для SQLite используем чисто-Go драйвер modernc.org/sqlite
# (не mattn/go-sqlite3 — тот требует CGO и усложняет кросс-сборку).
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
    -o /out/app ./cmd/${SERVICE}

# ============================================================
# Этап 2: финальный образ. Только рантайм, никаких тулчейнов.
# ============================================================
FROM alpine:3.24.3

# Свои непривилегированные пользователь и группа — контейнер не должен
# работать от root: даже если сервис скомпрометирован, прав у него минимум
RUN addgroup -S app && adduser -S -G app app

COPY --from=build /out/app /usr/local/bin/app

USER app

# Каждый сервис читает конфиг из env — CMD задаётся в compose,
# поэтому здесь только точка входа
ENTRYPOINT ["/usr/local/bin/app"]
