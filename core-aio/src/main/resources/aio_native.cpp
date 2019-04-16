#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <chrono>
#include <libaio.h>

#include "aio_native.h"

jint throwException(JNIEnv *jenv, int err) {
    jclass clazz = jenv->FindClass("java/io/IOException");
    if (clazz == NULL) {
        std::cerr << "Could not find class!\n";
        std::exit(-1);
    }
    return jenv->ThrowNew(clazz, strerror(err < 0 ? -err : err));
}

JNIEXPORT jlong JNICALL Java_io_datakernel_aio_nativeio_AioNative_io_1setup
(JNIEnv *jenv, jclass, jint nrEvents) {
    io_context_t ctx = NULL;
    int ret = io_setup(nrEvents, &ctx);
    if (ret) {
        throwException(jenv, ret);
    }

    return (jlong)ctx;
 }


JNIEXPORT jint JNICALL Java_io_datakernel_aio_nativeio_AioNative_io_1submit
(JNIEnv *jenv, jclass, jlong ctx, jint nr, jlong iocbpp) {

      iocb **ios = reinterpret_cast<iocb **>(iocbpp);
      int ret = io_submit((io_context_t)ctx, nr, ios);
      if (ret < 0) {
          return throwException(jenv, ret);
      }
      return ret;
 }


JNIEXPORT void JNICALL Java_io_datakernel_aio_nativeio_AioNative_destroy
(JNIEnv *jenv, jclass, jlong ctx) {
  int ret = io_destroy((io_context_t)ctx);
      if (ret) {
          throwException(jenv, ret);
      }
  }

JNIEXPORT void JNICALL Java_io_datakernel_aio_nativeio_AioNative_cancel
  (JNIEnv *jenv, jclass, jlong ctx, jlong iocbpp, jlong event) {
    iocb *io = reinterpret_cast<iocb *>(iocbpp);
    io_event *native_event = (io_event *)event;
    int ret = io_cancel((io_context_t)ctx, io, native_event);

    if (ret < 0) {
        throwException(jenv, ret);
    }
 }

JNIEXPORT jint JNICALL Java_io_datakernel_aio_nativeio_AioNative_io_1getevents
  (JNIEnv *jenv, jclass, jlong ctx, jlong min, jlong nr, jlong events, jlong milli_seconds) {
  io_event *native_events = (io_event *)events;
      int ret;
      if (milli_seconds < 0) {
          ret = io_getevents((io_context_t)ctx, min, nr, native_events, NULL);
      } else {
          using namespace std::chrono;
          milliseconds ms{ milli_seconds };
          seconds sec = duration_cast<seconds>(ms);
          timespec ts = { sec.count(),
              duration_cast<nanoseconds>(ms - sec).count()};
          ret = io_getevents((io_context_t)ctx, min, nr, native_events, &ts);
      }
      if (ret < 0) {
          return throwException(jenv, ret);
      }
      return ret;
  }


JNIEXPORT void JNICALL Java_io_datakernel_aio_nativeio_AioNative_io_1set_1eventfd
  (JNIEnv *jenv, jclass, jlong iocbpp, jint eventFD) {
        iocb **ios = reinterpret_cast<iocb **>(iocbpp);
        for (int i = 0; i < 0; i++) {
            iocb *io = ios[i];
            io_set_eventfd(io, eventFD);
        }
  }

