.PHONY: greeter telegram orders hello

greeter:
	cd examples/greeter && go run .

telegram:
	cd examples/telegram && go run .

orders:
	cd examples/orders && go run .

hello:
	cd examples/hello-system && go run .

ticketing:
	cd examples/ticketing && go run .
