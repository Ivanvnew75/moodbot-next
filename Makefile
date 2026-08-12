# Локальный запуск ТЕХ ЖЕ проверок, что и в CI.
#
# Зачем дублировать: проверка, которую можно запустить только пушем,
# заставляет отлаживать пайплайн коммитами «fix ci», «fix ci 2».
# Одна команда локально экономит часы и делает CI предсказуемым.
#
# Инварианта «одинаковые версии инструментов» здесь нет — локально
# ставится latest, в CI тоже latest. Для воспроизводимости версии
# следовало бы закрепить; для учебного проекта принято как есть.

GOBIN ?= $(shell go env GOPATH)/bin

.PHONY: check fmt vet lint sast sca secrets manifests tools

check: fmt vet lint sast sca secrets manifests
	@echo "\nвсе проверки пройдены"

tools:
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install github.com/securego/gosec/v2/cmd/gosec@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest

fmt:
	@echo "== gofmt =="
	@unformatted=$$(gofmt -l .); \
	 if [ -n "$$unformatted" ]; then echo "не отформатированы:"; echo "$$unformatted"; exit 1; fi

vet:
	@echo "== go vet =="
	@go vet ./...

lint:
	@echo "== staticcheck =="
	@$(GOBIN)/staticcheck ./...

sast:
	@echo "== gosec =="
	@$(GOBIN)/gosec -quiet -exclude-generated -severity medium ./...

sca:
	@echo "== govulncheck =="
	@$(GOBIN)/govulncheck ./...
	@echo "== trivy fs =="
	@trivy fs --scanners vuln --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1 --quiet .

secrets:
	@echo "== gitleaks (включая историю) =="
	@gitleaks detect --no-banner --redact

manifests:
	@echo "== trivy config по ОТРЕНДЕРЕННЫМ манифестам =="
	@kubectl kustomize deploy/overlays/prod > /tmp/rendered-prod.yaml
	@trivy config --severity HIGH,CRITICAL --exit-code 1 --quiet /tmp/rendered-prod.yaml
