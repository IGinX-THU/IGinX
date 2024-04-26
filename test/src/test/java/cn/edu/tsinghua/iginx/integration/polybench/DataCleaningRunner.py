import subprocess

# 生成 zipcode -> city的映射关系，error rate 10%
# lineNum 为 -n 后面的参数
import argparse
parser = argparse.ArgumentParser(description='generate zipcode -> city data for test')
parser.add_argument('-n', dest='lineNum', type=int, default=1000000,
                    help='line number of the generated data file, default 1000000')

args = parser.parse_args()
print('Data generated! Line number: ', args.lineNum)
if args.lineNum < 0:
    print('LineNum should be a positive number')
    exit(1)
elif args.lineNum > 10000000:
    print('LineNum too big, should be less than 10000000')
    exit(1)

correctNum = args.lineNum - args.lineNum // 10

zipcodes = [i for i in range(correctNum)]
cities = ['city' + str(i) for i in range(args.lineNum)]
# zipcodes加入10%的重复
zipcodes = zipcodes + [zipcodes[i] for i in range(args.lineNum // 10)]
with open('dataCleaning/zipcode_city.csv', 'w') as f:
    for i in range(args.lineNum):
        f.write(str(i) + ',' + cities[i] + ',' + str(zipcodes[i]) + '\n')

# 要运行的 shell 脚本文件路径
script_path = "./.github/scripts/benchmarks/dataCleaning.sh"

# 使用 subprocess.run() 运行 shell 脚本
# shell=True 表示通过 shell 解释器执行脚本
# 如果脚本有输出，可以通过 stdout=subprocess.PIPE 捕获输出
result = subprocess.run(script_path, shell=True, stdout=subprocess.PIPE, text=True)

# 检查脚本是否成功执行
if result.returncode == 0:
    print("Shell 脚本执行成功！")
    # 如果脚本有输出，可以打印输出内容
    if result.stdout:
        print("脚本输出：", result.stdout)
    # TODO 正确性检验
else:
    print("Shell 脚本执行失败")
