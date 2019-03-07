from functools import wraps
import time

def decorator(func):
    # print("initial cache for %s" % func.__name__)
    cache = {}

    @wraps(func)
    def decorated_func(*args, **kwargs):
        # 函数的名称作为key
        key = func.__name__
        result = None
        # 判断是否存在缓存
        if key in cache.keys():
            (result, updateTime) = cache[key]
            # 过期时间固定为10秒
            if time.time() - updateTime < 10:
                # print("limit call 10s", key)
                result = updateTime
            else:
                # print("cache expired !!! can call ")
                result = None
        else:
            # print("no cache for ", key)
            pass
        # 如果过期，或则没有缓存调用方法
        if result is None:
            result = func(*args, **kwargs)
            cache[key] = (result, time.time())
        return result

    return decorated_func



@decorator
def print_():
    print('成功保存数据库')

if __name__ == '__main__':
    decorator(print_)
    print_()


