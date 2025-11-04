.PHONY: unit-tests inte-tests clean

fs:
	@if [ -d build ]; then rm -rf build; fi
	@mkdir -p build
	@cd build && cmake ..
	@cd build && $(MAKE) -j
	@cd build && $(MAKE) build-tests -j

unit-tests:
	@if [ -d build ]; then rm -rf build; fi
	@mkdir -p build
	@cd build && cmake ..
	@cd build && $(MAKE) -j
	@cd build && $(MAKE) build-tests -j
	@cd build && $(MAKE) test -j

stress:
	@if [ -d build ]; then rm -rf build; fi
	@mkdir -p build
	@cd build && cmake ..
	@cd build && $(MAKE) -j
	@cd build && $(MAKE) build-tests -j
	@cd build && $(MAKE) run_concurrent_stress_test

clean:
	@rm -rf build