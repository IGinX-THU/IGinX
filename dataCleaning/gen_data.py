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