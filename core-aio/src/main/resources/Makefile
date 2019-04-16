
aio_native.so: aio_native.o
	@g++ -shared -fPIC aio_native.o -lc -laio -o aio_native.so

aio_native.o: aio_native.cpp aio_native.h
	@g++ -c -fPIC -I /usr/lib/jvm/java-8-oracle/include/ -I /usr/lib/jvm/java-8-oracle/include/linux/ aio_native.cpp -o aio_native.o

clean:
	@rm aio_native.o aio_native.so

