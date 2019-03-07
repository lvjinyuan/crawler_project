import os

a = os.path.dirname(os.path.abspath(__file__))    # 获取当前文件目录
# current_dir = os.path.abspath(os.path.join(os.getcwd(), "../..")) # 获取上上级目录
current_dir = os.path.abspath(os.path.join(os.getcwd(), ".."))  # 获取上级目录

