import datetime
import os
import time


def timer(func):
    # 定义一个装饰器函数，用来计算函数执行时间
    def wrapper(*args, **kwargs):
        # 记录函数开始执行的时间
        start_time = time.time()
        # 执行函数并获取返回值
        result = func(*args, **kwargs)
        # 记录函数结束执行的时间
        end_time = time.time()
        # 打印函数执行时间
        print(f"{func.__name__} took {end_time - start_time:.2f} seconds to execute.")
        # 返回函数执行结果
        return result

    # 返回装饰器函数
    return wrapper


def log_results(log_file_path):
    # 定义装饰器函数
    def decorator(func):
        # 定义包装器函数
        def wrapper(*args, **kwargs):
            # 调用原始函数
            result = func(*args, **kwargs)
            # 创建日志文件夹
            os.makedirs(log_file_path, exist_ok=True)
            # 获取当前时间
            now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # 打开日志文件，追加模式，utf-8编码
            with open(log_file_path, "a", encoding="utf-8") as log_file:
                # 写入日志
                log_file.write(f"{now_time} {func.__name__} - Result: {result}\n")
            # 返回原始函数结果
            return result

        # 返回包装器函数
        return wrapper

    # 返回装饰器函数
    return decoratorn(log_file_path, "a", encoding="utf-8") as log_file:
                # 写入日志
                log_file.write(f"{now_time} {func.__name__} - Result: {result}\n")
            # 返回原始函数结果
            return result

        # 返回包装器函数
        return wrapper

    # 返回装饰器函数
    return decorator


# 定义一个装饰器函数suppress_errors，用于捕获并抑制函数执行中的错误
def suppress_errors(func):
    # 定义一个内部函数wrapper，用于接收函数的参数并执行函数
    def wrapper(*args, **kwargs):
        # 尝试执行函数，并捕获可能发生的异常
        try:
            # 如果函数执行成功，则返回函数的结果
            return func(*args, **kwargs)
        # 如果发生异常，则打印异常信息，并返回None
        except Exception as e:
            print(f"Error in {func.__name__}: {e}")
            return None

    # 返回wrapper函数
    return wrapper


def retry(max_attempts, delay):
    # 定义一个装饰器函数
    def decorator(func):
        # 定义一个包装器函数
        def wrapper(*args, **kwargs):
            # 初始化尝试次数
            attempts = 0
            # 当尝试次数小于最大尝试次数时，循环执行
            while attempts < max_attempts:
                try:
                    # 尝试执行函数
                    return func(*args, **kwargs)
                except Exception as e:
                    # 如果执行失败，打印尝试次数和延迟时间
                    print(
                        f"Attempt {attempts + 1} failed. Retrying in {delay} seconds."
                    )
                    # 增加尝试次数
                    attempts += 1
                    # 延迟
                    time.sleep(delay)
            # 如果超过最大尝试次数，抛出异常
            raise Exception("Max retry attempts exceeded.")

        # 返回包装器函数
        return wrapper

    # 返回装饰器函数
    return decorator


# 定义一个debug函数，用来调试函数
def debug(func):
    def wrapper(*args, **kwargs):
        print(
            f"Debugging {func.__name__} - args: {args}, kwargs: {kwargs}"
        )  # 打印函数的参数
        return func(*args, **kwargs)  # 执行函数

    return wrapper
