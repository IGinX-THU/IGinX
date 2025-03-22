#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
# TSIGinX@gmail.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#

import ctypes
import threading
import functools
import inspect


class TimeoutError(Exception):
    pass


def _async_raise(thread_id, exctype):
    """
    往对应的线程中注入一个exception

    TODO: 或许可以在pemja C部分实现这个脚本，但pemja C 代码里对Python函数的调用太细碎了，需要花时间重写。
        所以先在python层面实现快一点。（或者直接在pemja添加python代码？）
    """
    result = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(thread_id),
        ctypes.py_object(exctype)
    )
    # 注入出错，取消
    if result > 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(thread_id),
            ctypes.py_object(None)
        )
        return 0
    return result


def get_thread_id(thread):
    if hasattr(thread, '_thread_id'):
        return thread._thread_id

    if hasattr(thread, 'native_id') and thread.native_id is not None:
        return thread.native_id

    # 旧版本Python没有前面的属性，使用旧的方法
    for tid, tobj in threading._active.items():
        if tobj is thread:
            return tid

    raise ValueError("cannot find thread ID")


class ThreadWithTimeout:
    """运行一个函数并进行超时管理"""
    def __init__(self, target, args=(), kwargs=None):
        self.target = target
        self.args = args
        self.kwargs = kwargs or {}
        self.result = None
        self.exception = None
        self.thread = None
        self.has_timeout = False

    def _thread_target(self):
        """运行函数"""
        try:
            self.result = self.target(*self.args, **self.kwargs)
        except Exception as e:
            self.exception = e

    def terminate(self):
        """超时，注入exception来终止"""
        if self.thread and self.thread.is_alive():
            thread_id = get_thread_id(self.thread)
            self.has_timeout = True
            # 尝试调用目标对象的interrupt方法，先进行中断操作
            target_object = getattr(self.target, '__self__', None)
            if target_object and hasattr(target_object, 'interrupt'):
                try:
                    print("interrupting...")
                    target_object.interrupt()
                except:
                    pass

            # 注入异常
            result = _async_raise(thread_id, TimeoutError)
            return result > 0
        return False

    def run_with_timeout(self, timeout):
        """创建一个线程来运行，监测超时"""
        self.thread = threading.Thread(target=self._thread_target)
        self.thread.daemon = True  # 保证至少能退出
        self.thread.start()
        self.thread.join(timeout)

        # 超时了
        if self.thread.is_alive():
            # 尝试终止，干预起效的话，线程会抛出TimeoutError
            success = self.terminate()
            self.thread.join(0.5)
            if self.thread.is_alive():
                # 无法终止，只能警告。
                # （只在JNI调用的时候）也有可能是线程在sleep，sleep之后会检测到异常
                raise TimeoutError(f"Thread timeout(>{timeout}s) with no respond to termination attempt. " + \
                                   f"Or the Thread is Possibly sleeping.")
            elif not success:
                raise TimeoutError(f"Thread timeout(>{timeout}s). Failed to terminate.")

        # 有异常，抛出
        if self.exception:
            if isinstance(self.exception, TimeoutError) and self.has_timeout:
                raise TimeoutError(f"Thread timeout(>{timeout}s) and is terminated.") from self.exception
            raise self.exception

        return self.result


def timeout(seconds):
    """一个装饰器，可以修饰需要被超时管理的函数"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 生成器函数，需要用生成器的方式进行包装
            if inspect.isgeneratorfunction(func):
                return timeout_generator(func, seconds)(*args, **kwargs)
            # 普通函数
            thread_wrapper = ThreadWithTimeout(func, args, kwargs)
            return thread_wrapper.run_with_timeout(seconds)
        return wrapper
    return decorator


def timeout_generator(gen_func, seconds):
    """装饰有超时管理的生成器函数"""
    @functools.wraps(gen_func)
    def wrapper(*args, **kwargs):
        g = gen_func(*args, **kwargs)

        @functools.wraps(gen_func)
        def next_with_timeout():
            thread_wrapper = ThreadWithTimeout(next, (g,))
            return thread_wrapper.run_with_timeout(seconds)

        # 每次迭代都进行超时管理
        try:
            while True:
                item = next_with_timeout()
                yield item
        except StopIteration:
            return

    return wrapper


class TimeoutSafeWrapper:
    """
    给一个实例创建一个安全包装，用例：safe_instance = TimeoutSafeWrapper(originalClass(), default_timeout=5)
    那么safe_instance里面的方法被调用时都会有安全管理
    考虑到数据处理分析可能耗时比较久，默认值设为30min
    """
    def __init__(self, original_object, default_timeout=30*60):
        self.original = original_object
        self.default_timeout = default_timeout

    def __getattr__(self, name):
        attr = getattr(self.original, name)

        # 如果是函数调用，进行超时管理
        if callable(attr):
            original_self = getattr(attr, '__self__', None) # 保存self
            def wrapped_method(*args, **kwargs):
                return attr(*args, **kwargs)
            wrapped_method.__self__ = original_self
            wrapped_method = timeout(self.default_timeout)(wrapped_method)
            return wrapped_method

        return attr

