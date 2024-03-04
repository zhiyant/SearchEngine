# 打开result.txt文件
with open("result.txt", "r") as file:
    # 读取文件内容
    data = file.read()

# 使用\n将字符串分割成项
items = data.split("\n")

# 创建一个空字典
result_dict = {}

# 遍历分割后的项
for item in items:
    # 使用逗号将键和值分割
    if len(item) > 10:
        key, value = item.rsplit(",", 1)
        result_dict[key] = float(value)  # 将值转换为浮点数

print(result_dict)

# 打开result3.txt文件
with open("result3.txt", "r") as file1:
    # 读取文件的每一行内容，并在末尾添加"value"，然后写入resultfinal.txt文件
    with open("resultfinal.txt", "w") as output_file:
        for line in file1:
            # 使用\001将行分割成单词列表
            words = line.split('\1',1)
            line = line.strip()  # 去除行尾的空白字符
            line = line + "\1" + str(result_dict[words[0]]) + "\r\n"  # 在末尾添加"value"
            output_file.write(line)  # 写入resultfinal.txt文件

print("内容已经成功写入resultfinal.txt文件")