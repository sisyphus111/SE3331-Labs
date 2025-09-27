.PHONY: unit-tests inte-tests clean

unit-tests:
	@if [ -d build ]; then rm -rf build; fi
	@mkdir -p build
	@cd build && cmake ..
	@cd build && $(MAKE) build-tests -j
	@cd build && $(MAKE) fs -j
	@cd build && $(MAKE) build-tests -j
	@cd build && $(MAKE) test -j

inte-tests:
	@if [ -d build ]; then rm -rf build; fi
	@mkdir -p build
	@cd build && cmake ..
	@cd build && $(MAKE) build-tests -j
	@cd build && $(MAKE) fs -j
	@cd build && cd ../scripts && ./integration_test.sh

clean:
	@rm -rf build