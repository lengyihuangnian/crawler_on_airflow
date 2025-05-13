#!/usr/bin/env python3
import os

# 模板文件路径
template_path = os.path.join(os.path.dirname(__file__), "template_appium.service")

# 读取模板文件内容
with open(template_path, 'r') as f:
    template_content = f.read()

# 生成10个服务文件，端口从6010到6019
for port in range(6010, 6020):
    # 替换端口号
    service_content = template_content.replace("{PORT}", str(port))
    
    # 生成输出文件路径
    output_file = os.path.join(os.path.dirname(__file__), f"appium-{port}.service")
    
    # 写入新服务文件
    with open(output_file, 'w') as f:
        f.write(service_content)
    
    print(f"已生成服务文件: appium-{port}.service")

print("所有服务文件生成完成！")
