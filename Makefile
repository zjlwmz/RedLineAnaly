PY=/usr/bin/python3

.PHONY: bootstrap run test stop clean

bootstrap:
	bash scripts/bootstrap.sh

run:        ## 启动 RabbitMQ + 主节点 + 2 个 worker（前台）
	WORKERS=2 PORT=8001 bash scripts/run_all.sh

test:       ## 端到端测试（需先 make run）
	$(PY) tests/e2e_test.py

stop:
	-pkill -f "uvicorn app.main"
	-pkill -f "app.worker --id"
	-docker stop redline-rmq >/dev/null 2>&1 || true

clean: stop
	rm -rf data/redline data/province/*.gpkg
