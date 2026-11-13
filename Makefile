up:
	@setsid cargo run > server_output.log 2>&1 & echo $$! > .server.pid
	@setsid cargo run --bin web_bridge > websocket_output.log 2>&1 & echo $$! > .web_bridge.pid
	@(cd web; setsid python -m http.server 3000 > ../webgateway_output.log 2>&1 & echo $$! > ../.webgateway.pid)

down:
	@for f in .server.pid .web_bridge.pid .webgateway.pid; do \
		if [ -f "$$f" ]; then \
			pid=$$(cat "$$f"); \
			kill -- -$$pid 2>/dev/null || true; \
			rm -f "$$f"; \
		fi; \
	done