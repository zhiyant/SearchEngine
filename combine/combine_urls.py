import os

root='.'
path='.'
files=os.listdir(path)
result=os.path.join(root,'resultURL.txt') #生成最终txt文件(result.txt)的路径
        
with open(result,'w',encoding='utf-8-sig') as r:
    for i in range(8,17):
        print(i)
        fname='result'+str(i)+'.txt'
        filename=os.path.join(path,fname)
        with open(filename,'r',encoding='utf-8-sig') as f:
            for line in f:
                r.writelines(line)
            r.write('\r\n')