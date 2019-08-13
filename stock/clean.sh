#!/bin/sh

clean() {
  for file in $1/*    #读取log日志所在文件夹的所有信息
  do
    if [ -d $file ]
    then
      clean $file
    else
      echo $file
      temp=$(tail -100 $file)  #只保留文件末尾100行最新数据
      echo "$temp" > $file
    fi
  done
}

dir=logs  #这个dir路径设置要注意是log日志所在的文件夹，而不能是log日志本身，因为log是文件
clean $dir
