function make_build {
	mkdir ./build
	cp ../../../cmd/mosn/main/* ./build/
	cp credential.go ./build/
	cd ./build
	go build -o mosn
	mv mosn	../
	cd ../
	rm -rf ./build
}

make_build
